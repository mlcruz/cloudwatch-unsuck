use chrono::Utc;
use futures::stream::StreamExt;
use rusqlite::params;

use crate::open_sqlite;
pub struct Sync {
    pub pattern: String,
}

impl Sync {
    pub async fn sync(self) -> anyhow::Result<()> {
        let config = aws_config::load_from_env().await;
        let client = aws_sdk_cloudwatchlogs::Client::new(&config);

        let mut group_paginator = client
            .describe_log_groups()
            .log_group_name_pattern(&self.pattern)
            .into_paginator()
            .send();

        let mut sqlite_conn = open_sqlite();

        sqlite_conn.execute("DELETE FROM log_groups;", []).unwrap();
        sqlite_conn.execute("DELETE FROM log_events;", []).unwrap();

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

                let log_group_name = log_group.log_group_name().map(|s| s.to_string());
                let mut log_stream_paginator = client
                    .describe_log_streams()
                    .set_log_group_name(log_group_name.clone())
                    .into_paginator()
                    .send();

                let start_time = Utc::now() - chrono::Duration::days(1);
                while let Some(stream_description) =
                    log_stream_paginator.next().await.transpose()?
                {
                    for stream in stream_description.log_streams().unwrap_or_default() {
                        let mut logs_paginator = client
                            .get_log_events()
                            .set_start_time(start_time.timestamp_millis().into())
                            .set_log_group_identifier(log_group_name.clone())
                            .set_log_stream_name(Some(stream.log_stream_name.clone().unwrap()))
                            .into_paginator()
                            .send();

                        while let Some(description) = logs_paginator.next().await.transpose()? {
                            let log_events = description.events().unwrap_or_default();
                            let tx = sqlite_conn.transaction().unwrap();
                            let mut stmt = tx
                        .prepare_cached(
                            "INSERT INTO log_events (timestamp, message, ingestion_time, log_group_name)
                    VALUES (?, ?, ?, ?)",
                        )
                        .unwrap();

                            for event in log_events {
                                stmt.execute(params![
                                    event.timestamp,
                                    event.message,
                                    event.ingestion_time,
                                    log_group.log_group_name().unwrap()
                                ])
                                .unwrap();
                            }
                            std::mem::drop(stmt);
                            tx.commit().unwrap();
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
