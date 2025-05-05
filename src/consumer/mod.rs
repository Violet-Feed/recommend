use rdkafka::{ClientConfig, Message};
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use crate::handler::embedding_handler::handle_embedding_report;
use crate::handler::hotspot_handler::handle_hotspot_report;
use crate::dal::redis;
use crate::recommend::ReportMessage;
use crate::recommend::ReportType::*;
use rocketmq::conf::LoggingFormat::Json;
use rocketmq::conf::{ClientOption, SimpleConsumerOption};
use rocketmq::model::common::{FilterExpression, FilterType};
use rocketmq::SimpleConsumer;

pub async fn start_rocketmq() -> ! {
    tracing::info!("[start_rocketmq] Starting rocketmq client");
    let mut consumer_option = SimpleConsumerOption::default();
    consumer_option.set_topics(vec!["recommend"]);
    consumer_option.set_consumer_group("test");
    consumer_option.set_logging_format(Json);
    let mut client_option = ClientOption::default();
    client_option.set_access_url("localhost:8081");
    client_option.set_enable_tls(false);

    let mut consumer = SimpleConsumer::new(consumer_option, client_option).unwrap();
    if let Err(err) = consumer.start().await {
        panic!("[start_rocketmq] simple consumer start err. err = {:?}", err);
    }

    loop {
        let receive_result = consumer.receive("recommend".to_string(), &FilterExpression::new(FilterType::Tag, "*")).await;
        if receive_result.is_err() {
            tracing::error!("[start_rocketmq] receive message err. err = {:?}",receive_result.unwrap_err());
        } else {
            let messages = receive_result.unwrap();
            for message in messages {
                match serde_json::from_slice::<ReportMessage>(message.body()) {
                    Ok(report) => {
                        tracing::info!("[start_rocketmq] receive message. message = {:?}", report);
                        match report.report_type{
                            x if x == Embedding as i32 => {
                                if let Err(e) = handle_embedding_report(&report.namespace,report.embedding_report.unwrap_or_default()).await {
                                    tracing::error!("[start_rocketmq] handle_embedding_report err. err = {:?}", e);
                                    continue;
                                }
                            }
                            //TODO:use grpc streaming
                            x if x == HotSpot as i32 => {
                                if let Err(e) = handle_hotspot_report(&report.namespace,report.hotspot_report.unwrap_or_default()).await {
                                    tracing::error!("[start_rocketmq] handle_hotspot_report err. err = {:?}", e);
                                    continue;
                                }
                            }
                            _ => {
                                tracing::error!("[start_rocketmq] unknown report type. report_type = {}", report.report_type);
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!("[start_rocketmq] deserializing json err. err = {}", e);
                    }
                }
                if let Err(e) = consumer.ack(&message).await {
                    tracing::error!("[start_rocketmq] ack message err. err = {:?}", e);
                }
            }
        }
    }
}

pub async fn start_kafka() -> ! {
    tracing::info!("[start_kafka] Starting kafka client");
    let broker = "localhost:9092".to_string();
    let topic = "flink-topk-output";
    let group = "default".to_string();

    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", group)
        .set("bootstrap.servers", broker)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        //.set("auto.offset.reset", "smallest")
        .set_log_level(RDKafkaLogLevel::Info)
        .create()
        .expect("[start_kafka] consumer create err.");
    consumer.subscribe(&[topic]).expect("[start_kafka] subscribe to topics err.");

    loop {
        match consumer.recv().await {
            Ok(m) => {
                let payload = match m.payload_view::<str>() {
                    None => "",
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        tracing::error!("[start_kafka] deserializing message payload err. err = {:?}", e);
                        ""
                    }
                };
                let key = match m.key_view::<str>() {
                    None => "",
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        tracing::error!("[start_kafka] deserializing message key err. err = {:?}", e);
                        ""
                    }
                };
                tracing::info!("[start_kafka] receive message. key: '{:?}', payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
                      key, payload, m.topic(), m.partition(), m.offset(), m.timestamp());

                if payload != "" && key != "" {
                    if let Err(e) = redis::set_topk(key, payload).await{
                        tracing::error!("[start_kafka] redis set err. err = {:?}", e);
                        continue;
                    }
                }
                if let Err(e)=consumer.commit_message(&m, CommitMode::Async){
                    tracing::error!("[start_kafka] commit message err. err = {:?}", e);
                }
            }
            Err(e) => {
                tracing::error!("[start_kafka] receive message err. err = {}", e)
            }
        };
    }
}