use rocketmq::conf::{ClientOption, SimpleConsumerOption};
use rocketmq::conf::LoggingFormat::Json;
use rocketmq::model::common::{FilterExpression, FilterType};
use rocketmq::SimpleConsumer;

pub mod im{
    tonic::include_proto!("im");
}
use im::MessageEvent;

pub async fn start() -> Result<(), Box<dyn std::error::Error>> {
    tracing::info!("[start] Starting rocketmq client");
    let mut consumer_option = SimpleConsumerOption::default();
    consumer_option.set_topics(vec!["conversation"]);
    consumer_option.set_consumer_group("test");
    consumer_option.set_logging_format(Json);
    let mut client_option = ClientOption::default();
    client_option.set_access_url("localhost:8081");
    client_option.set_enable_tls(false);
    
    let mut consumer = SimpleConsumer::new(consumer_option, client_option)?;
    if let Err(err) = consumer.start().await {
        tracing::error!("[start] simple consumer start err. err = {:?}", err);
        return Err(err.into());
    }

    loop {
        let receive_result = consumer
            .receive("conversation".to_string(), &FilterExpression::new(FilterType::Tag, "*"), )
            .await;
        if receive_result.is_err() {
            tracing::error!("[start] receive message err. err = {:?}",receive_result.unwrap_err());
        }else{
            let messages = receive_result.unwrap();
            for message in messages {
                match serde_json::from_slice::<MessageEvent>(message.body()) {
                    Ok(message) => {
                        tracing::info!("[start] receive message. message = {:?}",message);
                    }
                    Err(e) => {
                        tracing::error!("[start] deserializing json err. err = {}", e);
                    }
                }
                let ack_result = consumer
                    .ack(&message)
                    .await;
                if ack_result.is_err() {
                    tracing::error!("[start] ack message err. err = {:?}",ack_result.unwrap_err());
                }
            }
        }
    }
}