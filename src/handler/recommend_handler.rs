use crate::recommend::RecommendRequest;
use crate::dal::{redis, milvus};
use anyhow::{Context, Result};

pub async fn handle_recommend_request(req:RecommendRequest) -> Result<String> {
    let item_history = redis::get_user_history(req.user_id).await
        .context("[handle_recommend_request] get_user_history err.")?;
    let mut embeddings = milvus::get_item_embedding(item_history).await
        .context("[handle_recommend_request] get_item_embedding err.")?;
    while embeddings.len() < 6 {
        embeddings.push(milvus::random_embedding());
    }
    let mut results = Vec::new();
    let mut step =0;
    while results.len()<20{
        step+=1;
        let mut new_results =milvus::recall_item(embeddings.clone(), step).await
            .context("[handle_recommend_request] recall_item err.")?;
        new_results.retain(|item| !results.contains(item));
        if new_results.len() == 0 {
            break;
        }
        // new_results=redis::execute_impression(req.user_id,new_results).await
        //     .context("[handle_recommend_request] execute_impression err.")?;
        results.append(&mut new_results);
    }
    results.truncate(20);
    // redis::write_impression(req.user_id,results.clone()).await
    //     .context("[handle_recommend_request] write_impression err.")?;
    let result = serde_json::to_string(&results)
        .context("[handle_recommend_request] serialize json err.")?;
    Ok(result)
}