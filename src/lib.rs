use std::{sync::Arc, thread::JoinHandle};

use anyhow::anyhow;
use crossbeam_channel::{bounded, Sender};
use log::error;
use rusqlite::{Connection, Transaction};
use serde_json::Value;

#[derive(Debug)]
pub struct SqliteTransactor {
    sender: Option<
        Sender<(
            Box<dyn Fn(&Transaction) -> anyhow::Result<Value> + Send + 'static>,
            Sender<anyhow::Result<Value>>,
        )>,
    >,
    join_handle: Option<JoinHandle<()>>,
}

impl SqliteTransactor {
    pub fn new(mut conn: Connection, cap: usize) -> Arc<SqliteTransactor> {
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

        Arc::new(Self {
            sender: Some(sender),
            join_handle: Some(h),
        })
    }

    pub fn execute(
        &self,
        f: Box<dyn Fn(&Transaction) -> anyhow::Result<Value> + Send + 'static>,
    ) -> anyhow::Result<Value> {
        let sender = self.sender.as_ref().ok_or(anyhow!("sender is None"))?;
        let (tx, rx) = bounded::<anyhow::Result<Value>>(0);
        sender.send((f, tx)).map_err(|e| anyhow!("{}", e))?;
        rx.recv().map_err(|e| anyhow!("{}", e))?
    }

    pub fn end(actor: Arc<SqliteTransactor>, handle: JoinHandle<()>) -> anyhow::Result<()> {
        drop(actor);
        Ok(handle.join().map_err(|e| anyhow!("{:?}", e))?)
    }
}

impl Drop for SqliteTransactor {
    fn drop(&mut self) {
        if let Some(sender) = self.sender.take() {
            drop(sender);
        }
        if let Some(handle) = self.join_handle.take() {
            if let Err(e) = handle.join() {
                error!("{e:?}");
            }
        }
    }
}
