use rusqlite::{Connection, Transaction};
use sqlite_transactor::SqliteTransactor;
use std::{sync::Arc, thread, time};

fn main() {
    let now = time::Instant::now();

    let conn = Connection::open(r"test.db").unwrap();

    conn.execute(
        r#"
    CREATE TABLE if not exists todos (
            id    INTEGER PRIMARY KEY,
            description  TEXT,
            done BOOL
            )
            "#,
        (),
    )
    .unwrap();

    let sql = format!(
        r#"
    INSERT INTO todos ( description ) VALUES ( "{}" )
        "#,
        "test"
    );

    let (actor, handle) = SqliteTransactor::begin(conn, 10);

    let mut hs = vec![];
    for _ in 0..10 {
        let sql = sql.clone();
        let actor = Arc::clone(&actor);
        let h = thread::spawn(move || {
            for _ in 0..1000000 {
                let sql = sql.clone();
                actor
                    .execute(Box::new(move |transaction: &Transaction| {
                        let _ = transaction.execute(&sql, ());
                        Ok(transaction.last_insert_rowid().into())
                    }))
                    .unwrap();

                // actor
                //     .execute(Box::new(move |transaction: &Transaction| {
                //         let mut stmt = transaction.prepare("select description from todos")?;
                //         let iter = stmt.query_map([], |row| row.get(0))?;
                //         let r: Vec<String> = iter.filter_map(|i| i.ok()).collect();
                //         Ok(serde_json::to_value(r)?)
                //     }))
                //     .unwrap();
            }
        });
        hs.push(h);
    }

    for h in hs {
        h.join().unwrap();
    }

    let _ = SqliteTransactor::end(actor, handle);

    println!("time: {:?}", time::Instant::now() - now);
}
