use crate::recommend::RecommendRequest;
use anyhow::Result;

pub async fn handle_recommend_request(req:RecommendRequest) -> Result<String> {
    Ok("success".to_string())
}