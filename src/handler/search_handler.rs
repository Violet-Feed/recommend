use crate::dal::milvus::{insert_event, recall_event, search_item};
use crate::dal::model::{call_event_embedding_model, call_event_model, call_multi_embedding_model};
use crate::recommend::SearchRequest;
use anyhow::{Context, Result};
use milvus::value::ValueVec;
use crate::hotspot::commit_hotspot;

pub async fn handle_search_request(req: SearchRequest) -> Result<String> {
    let embedding = call_multi_embedding_model(&vec![req.keyword.clone()], &vec![], &vec![]).await
        .context("[handle_search_request] call_multi_embedding_model err.")?;
    let results=search_item(embedding, &req.keyword, req.page).await
        .context("[handle_search_request] search_item err.")?;
    let result= serde_json::to_string(&results)
        .context("[handle_search_request] serialize json err.")?;
    tokio::spawn(async move {
        if let Err(e) = report_keyword(&req.namespace, &req.keyword).await {
            tracing::error!("[handle_search_request] report_keywords err: {:?}", e);
        }
    });
    Ok(result)
}

async fn report_keyword(namespace: &str, keyword: &str) -> Result<()> {
    let event = call_event_model(keyword).await
        .context("[report_keyword] call_event_model err.")?;
    tracing::info!("[report_keyword] call_event_model. event = {:?}", event);

    let embedding = call_event_embedding_model(&event).await
        .context("[report_keyword] call_event_embedding_model err.")?;

    if let Some(result) = recall_event(embedding.clone()).await
        .context("[report_keyword] recall_event err.")? {
        let is_exist = result.score.first().copied().map_or(false, |score| score > 0.8);
        if is_exist {
            if let Some(ValueVec::String(event_name)) = result.field.first().map(|c| &c.value) {
                if let Some(exist_event) = event_name.get(0).cloned() {
                    commit_hotspot(namespace, &exist_event).await?;
                    tracing::info!("[report_keyword] commit_hotspot exist event = {}", exist_event);
                    return Ok(());
                }
            }
        }
    }
    insert_event(&event, embedding).await
        .context("[report_keyword] insert_event err.")?;
    commit_hotspot(namespace, &event).await?;
    tracing::info!("[report_keyword] commit_hotspot new event = {}", event);
    Ok(())
}