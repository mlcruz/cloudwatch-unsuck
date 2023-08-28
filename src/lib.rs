pub mod describe;

use clap::{Parser, Subcommand};
use serde_json::json;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    Sync {
        #[arg(short, long)]
        pattern: String,
    },
}

pub fn open_sqlite() -> rusqlite::Connection {
    let home_dir = dirs::home_dir().unwrap();
    let path = home_dir.join("cw-unsuck/db.sqlite");
    std::fs::remove_file(&path).ok();
    std::fs::create_dir_all(path.parent().unwrap()).ok();
    let mut conn = rusqlite::Connection::open(path).unwrap();
    create_tables(&mut conn);

    conn
}

pub fn create_tables(conn: &mut rusqlite::Connection) {
    conn.execute(
        "
    CREATE TABLE IF NOT EXISTS log_groups (
        id INTEGER PRIMARY KEY,
        log_group_name TEXT,
        creation_time INTEGER,
        retention_in_days INTEGER,
        metric_filter_count INTEGER,
        arn TEXT,
        stored_bytes INTEGER,
        kms_key_id TEXT
    );",
        [],
    )
    .unwrap();

    conn.execute(
        "CREATE TABLE IF NOT EXISTS log_events (
            id INTEGER PRIMARY KEY,
            timestamp INTEGER,
            message TEXT,
            ingestion_time INTEGER,
            log_group_name TEXT
        );
    ",
        [],
    )
    .unwrap();

    conn.execute(
        "CREATE INDEX IF NOT EXISTS log_group_name_idx ON log_events (log_group_name);",
        [],
    )
    .unwrap();
}

pub fn json_format(text: String) -> std::string::String {
    if text.starts_with('{') {
        return text;
    }

    let json_start = text.find('{');

    match json_start {
        Some(idx) => {
            let (before_json, json) = text.split_at(idx - 1);
            let json_obj: Result<serde_json::Value, _> = serde_json::from_str(json);
            match json_obj {
                Ok(json) => json!(
                    {"data": json,
                    "text": before_json}
                )
                .to_string(),
                Err(_) => {
                    eprintln!("err: {}", &text);
                    text
                }
            }
        }
        None => text,
    }
}
