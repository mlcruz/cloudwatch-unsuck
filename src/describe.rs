use futures::stream::StreamExt;
use rusqlite::params;

use crate::open_sqlite;
pub struct Sync {}

impl Sync {
    pub async fn sync(self) -> anyhow::Result<()> {
        let config = aws_config::load_from_env().await;
        let client = aws_sdk_cloudwatchlogs::Client::new(&config);

        let mut group_paginator = client.describe_log_groups().into_paginator().send();

        let sqlite_conn = open_sqlite();

        sqlite_conn.execute("DELETE FROM log_groups;", []).unwrap();
        while let Some(description) = group_paginator.next().await.transpose()? {
            for log_group in description.log_groups().unwrap_or_default() {
                let query = "INSERT INTO log_groups (
                    log_group_name, 
                    creation_time, 
                    retention_in_days, 
                    metric_filter_count, 
                    arn, 
                    stored_bytes, 
                    kms_key_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?);";

                sqlite_conn.execute(
                    query,
                    params![
                        log_group.log_group_name,
                        log_group.creation_time,
                        log_group.retention_in_days,
                        log_group.metric_filter_count,
                        log_group.arn,
                        log_group.stored_bytes,
                        log_group.kms_key_id,
                    ],
                )?;
            }
        }

        Ok(())
    }
}
