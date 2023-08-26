pub mod describe;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    Sync {},
}

pub fn open_sqlite() -> rusqlite::Connection {
    let home_dir = dirs::home_dir().unwrap();
    let path = home_dir.join("cw-unsuck/db.sqlite");
    std::fs::create_dir_all(path.parent().unwrap()).ok();
    let mut conn = rusqlite::Connection::open(path).unwrap();
    create_tables(&mut conn);

    conn
}

pub fn create_tables(conn: &mut rusqlite::Connection) {
    let query = "
    CREATE TABLE IF NOT EXISTS log_groups (
        log_group_name TEXT,
        creation_time INTEGER,
        retention_in_days INTEGER,
        metric_filter_count INTEGER,
        arn TEXT,
        stored_bytes INTEGER,
        kms_key_id TEXT
    );
    ";
    conn.execute(query, []).unwrap();
}
