mod handler;
mod consumer;
mod dal;
mod hotspot;

pub mod common {
    tonic::include_proto!("common");
}
pub mod recommend {
    tonic::include_proto!("recommend");
}

use tonic::{transport::Server, Request, Response, Status};
use common::{BaseResp, StatusCode};
use recommend::recommend_service_server::{RecommendService, RecommendServiceServer};
use recommend::{RecommendRequest, RecommendResponse};

#[derive(Debug, Default)]
pub struct MyRecommendService {}

#[tonic::async_trait]
impl RecommendService for MyRecommendService {
    async fn recommend(
        &self,
        request: Request<RecommendRequest>,
    ) -> Result<Response<RecommendResponse>, Status> {
        let req = request.into_inner();
        let keys = vec!["key1".to_string(), "key2".to_string()];
        let base_resp = BaseResp {
            status_code: StatusCode::Success as i32,
            status_message: "Success".to_string(),
        };
        let response = RecommendResponse {
            keys,
            base_resp: Some(base_resp),
        };
        Ok(Response::new(response))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    tokio::spawn(async move {
        if let Err(e) = consumer::rocketmq::start().await {
            tracing::error!("[main] rocketmq consumer error. err = {}", e);
        }
    });
    
    let addr = "127.0.0.1:3006".parse()?;
    let recommend_service = MyRecommendService::default();
    Server::builder()
        .add_service(RecommendServiceServer::new(recommend_service))
        .serve(addr)
        .await?;
    Ok(())
}    