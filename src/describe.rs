use chrono::{NaiveDateTime, Utc};
use futures::stream::StreamExt;
use rusqlite::params;

use crate::{json_format, open_sqlite};
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
                println!("syncing log group: {}", log_group.log_group_name().unwrap());
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
                    .descending(true)
                    .set_log_group_name(log_group_name.clone())
                    .into_paginator()
                    .items()
                    .send();

                let start_time = Utc::now() - chrono::Duration::days(1);
                while let Some(stream) = log_stream_paginator.next().await.transpose()? {
                    let mut logs_paginator = client
                        .get_log_events()
                        .set_start_time(start_time.timestamp_millis().into())
                        .set_log_group_name(log_group_name.clone())
                        .set_log_stream_name(Some(stream.log_stream_name.clone().unwrap()))
                        .into_paginator()
                        .send();

                    if let Some(last_event) = stream.last_ingestion_time() {
                        if last_event < start_time.timestamp_millis() {
                            break;
                        }
                    } else {
                        println!("breaking {}", stream.log_stream_name().unwrap());
                        break;
                    }

                    let tx = sqlite_conn.transaction().unwrap();
                    let mut stmt = tx
                    .prepare_cached(
                        "INSERT INTO log_events (timestamp, message, ingestion_time, log_group_name)
                VALUES (?, ?, ?, ?)",
                    )
                    .unwrap();

                    let mut count = 0;

                    while let Some(description) = logs_paginator.next().await.transpose()? {
                        let log_events = description.events().unwrap_or_default();

                        for event in log_events {
                            count += 1;
                            stmt.execute(params![
                                event.timestamp,
                                json_format(event.message.clone().unwrap_or_default()),
                                event.ingestion_time,
                                log_group.log_group_name().unwrap()
                            ])
                            .unwrap();
                        }
                    }
                    println!(
                        "{} {} {}",
                        NaiveDateTime::from_timestamp_millis(stream.creation_time().unwrap())
                            .unwrap(),
                        NaiveDateTime::from_timestamp_millis(
                            stream.last_ingestion_time().unwrap_or_default()
                        )
                        .unwrap(),
                        { count }
                    );
                    std::mem::drop(stmt);
                    tx.commit().unwrap();
                }
            }
        }

        Ok(())
    }
}
