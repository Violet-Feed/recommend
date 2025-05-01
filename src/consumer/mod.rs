use crate::handler::embedding_handler::handle_embedding_report;
use crate::handler::behavior_handler::handle_behavior_report;
use crate::recommend::ReportMessage;
use crate::recommend::ReportType::*;
use rocketmq::conf::LoggingFormat::Json;
use rocketmq::conf::{ClientOption, SimpleConsumerOption};
use rocketmq::model::common::{FilterExpression, FilterType};
use rocketmq::SimpleConsumer;

pub async fn start() -> ! {
    tracing::info!("[start] Starting rocketmq client");
    let mut consumer_option = SimpleConsumerOption::default();
    consumer_option.set_topics(vec!["recommend"]);
    consumer_option.set_consumer_group("test");
    consumer_option.set_logging_format(Json);
    let mut client_option = ClientOption::default();
    client_option.set_access_url("localhost:8081");
    client_option.set_enable_tls(false);

    let mut consumer = SimpleConsumer::new(consumer_option, client_option).unwrap();
    if let Err(err) = consumer.start().await {
        panic!("[start] simple consumer start err. err = {:?}", err);
    }

    loop {
        let receive_result = consumer.receive("recommend".to_string(), &FilterExpression::new(FilterType::Tag, "*")).await;
        if receive_result.is_err() {
            tracing::error!("[start] receive message err. err = {:?}",receive_result.unwrap_err());
        } else {
            let messages = receive_result.unwrap();
            for message in messages {
                match serde_json::from_slice::<ReportMessage>(message.body()) {
                    Ok(report) => {
                        tracing::info!("[start] receive message. message = {:?}", report);
                        match report.report_type{
                            x if x == Embedding as i32 => {
                                if let Err(e) = handle_embedding_report(&report.namespace,report.embedding_report.unwrap_or_default()).await {
                                    tracing::error!("[start] handle_embedding_report err. err = {:?}", e);
                                    continue;
                                }
                            }
                            x if x == Behavior as i32 => {
                                if let Err(e) = handle_behavior_report(&report.namespace,report.behavior_report.unwrap_or_default()).await {
                                    tracing::error!("[start] handle_behavior_report err. err = {:?}", e);
                                    continue;
                                }
                            }
                            _ => {
                                tracing::error!("[start] unknown report type. report_type = {}", report.report_type);
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!("[start] deserializing json err. err = {}", e);
                    }
                }
                if let Err(e) = consumer.ack(&message).await {
                    tracing::error!("[start] ack message err. err = {:?}", e);
                }
            }
        }
    }
}