use tonic::{transport::Server, Request, Response, Status};

pub mod common {
    tonic::include_proto!("common");
}
pub mod recommend {
    tonic::include_proto!("recommend");
}


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
    let addr = "127.0.0.1:3006".parse()?;
    let recommend_service = MyRecommendService::default();
    Server::builder()
        .add_service(RecommendServiceServer::new(recommend_service))
        .serve(addr)
        .await?;
    Ok(())
}    