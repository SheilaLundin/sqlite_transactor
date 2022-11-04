use std::{sync::Arc, thread::JoinHandle};

use anyhow::anyhow;
use crossbeam_channel::{bounded, Sender};
use log::error;
use rusqlite::{Connection, Transaction};
use serde_json::Value;

#[derive(Debug)]
pub struct SqliteTransactor {
    sender: Sender<(
        Box<dyn Fn(&Transaction) -> anyhow::Result<Value> + Send + 'static>,
        Sender<anyhow::Result<Value>>,
    )>,
}

impl SqliteTransactor {
    pub fn new(mut conn: Connection, cap: usize) -> (Arc<SqliteTransactor>, JoinHandle<()>) {
        let (sender, receiver) = bounded::<(
            Box<dyn Fn(&Transaction) -> anyhow::Result<Value> + Send + 'static>,
            Sender<anyhow::Result<Value>>,
        )>(cap);

        let h = std::thread::spawn(move || {
            match conn.transaction() {
                Ok(transaction) => {
                    for (f, tx) in receiver.iter() {
                        let r = f(&transaction);
                        if let Err(e) = tx.send(r) {
                            error!("{}", e);
                        }
                        drop(tx);
                    }
                    if let Err(e) = transaction.commit() {
                        error!("{}", e);
                    }
                }
                Err(e) => {
                    error!("{}", e);
                }
            }
            if let Err(e) = conn.close() {
                error!("{:?}", e);
            }
        });

        (Arc::new(Self { sender }), h)
    }

    pub fn execute(
        &self,
        f: Box<dyn Fn(&Transaction) -> anyhow::Result<Value> + Send + 'static>,
    ) -> anyhow::Result<Value> {
        let (tx, rx) = bounded::<anyhow::Result<Value>>(0);
        self.sender.send((f, tx)).map_err(|e| anyhow!("{}", e))?;
        rx.recv().map_err(|e| anyhow!("{}", e))?
    }

    pub fn commit(actor: Arc<SqliteTransactor>, handle: JoinHandle<()>) -> anyhow::Result<()> {
        drop(actor);
        Ok(handle.join().map_err(|e| anyhow!("{:?}", e))?)
    }
}
