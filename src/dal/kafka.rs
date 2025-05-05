use std::time::Duration;
use anyhow::{Context, Error, Result};
use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde_json::json;
use tokio::sync::OnceCell;

static FLINK_PRODUCER:OnceCell<FutureProducer> = OnceCell::const_new();

pub async fn get_flink_producer() -> &'static FutureProducer {
    FLINK_PRODUCER.get_or_init(|| async {
        let broker = "localhost:9092".to_string();
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", broker)
            .set("message.timeout.ms", "5000")
            .create()
            .expect("Producer creation error");
        producer
    }).await
}

pub async fn send_to_flink(namespace:&str, key:&str) -> Result<()> {
    let producer= get_flink_producer().await;
    let topic = "flink-topk-input";
    let data = json!({
        "namespace": namespace,
        "key": key,
    });
    let data_str = serde_json::to_string(&data)
        .context("[send_to_flink] serializing json err.")?;
    producer.send(
        FutureRecord::to(topic)
            .key(namespace)
            .payload(&data_str),
        Duration::from_secs(0),
    ).await
    .map_err(|(err, _)| Error::new(err)
        .context("[send_to_flink] send message to kafka err."))?;
    Ok(())
}