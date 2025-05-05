mod consumer;
mod dal;
mod handler;
mod hotspot;

pub mod common {
    tonic::include_proto!("common");
}
pub mod recommend {
    tonic::include_proto!("recommend");
}

use common::{BaseResp, StatusCode};
use recommend::recommend_service_server::{RecommendService, RecommendServiceServer};
use recommend::{RecommendRequest, RecommendResponse};
use recommend::{SearchRequest, SearchResponse};
use tonic::{transport::Server, Request, Response, Status};
use handler::recommend_handler::handle_recommend_request;
use handler::search_handler::handle_search_request;

#[derive(Debug, Default)]
pub struct MyRecommendService {}

#[tonic::async_trait]
impl RecommendService for MyRecommendService {
    async fn recommend(
        &self,
        request: Request<RecommendRequest>,
    ) -> Result<Response<RecommendResponse>, Status> {
        let req = request.into_inner();
        match handle_recommend_request(req).await{
            Ok(results) => {
                let base_resp = BaseResp {
                    status_code: StatusCode::Success as i32,
                    status_message: "Success".to_string(),
                };
                let response = RecommendResponse {
                    results,
                    base_resp: Some(base_resp),
                };
                Ok(Response::new(response))
            }
            Err(e) => {
                tracing::error!("[recommend] handle_recommend_request err. err = {}", e);
                Err(Status::internal(format!("Error: {}", e)))
            }
        }
    }

    async fn search(
        &self,
        request: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        let req = request.into_inner();
        match handle_search_request(req).await{
            Ok(results) => {
                let base_resp = BaseResp {
                    status_code: StatusCode::Success as i32,
                    status_message: "Success".to_string(),
                };
                let response = SearchResponse {
                    results,
                    base_resp: Some(base_resp),
                };
                Ok(Response::new(response))
            }
            Err(e) => {
                tracing::error!("[search] handle_search_request err. err = {}", e);
                Err(Status::internal(format!("Error: {}", e)))
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    sentinel_core::init_default().expect("Failed to initialize Sentinel");

    tokio::spawn(async move {
        consumer::start_rocketmq().await;
    });
    // tokio::spawn(async move {
    //     consumer::start_kafka().await;
    // });

    let addr = "127.0.0.1:3006".parse()?;
    let recommend_service = MyRecommendService::default();
    Server::builder()
        .add_service(RecommendServiceServer::new(recommend_service))
        .serve(addr)
        .await?;
    Ok(())
}
