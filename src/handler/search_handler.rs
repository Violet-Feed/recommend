use crate::dal::milvus::{search_event_embedding, store_event_embedding};
use crate::dal::model::{call_event_embedding_model, call_event_model};
use crate::recommend::ReportMessage;
use anyhow::{Context, Result};
use milvus::value::ValueVec;
use crate::hotspot::commit_hotspot;

pub async fn handle_search_report(message: ReportMessage) -> Result<()> {
    let event = call_event_model(&message.key).await
        .context("[handle_search_report] call_event_model err.")?;
    tracing::info!("[handle_search_report] call_event_model. event = {:?}", event);

    let embedding = call_event_embedding_model(&event).await
        .context("[handle_search_report] call_event_embedding_model err.")?;

    if let Some(result) = search_event_embedding(embedding.clone()).await
        .context("[handle_search_report] search_event_embedding err.")? {
        let is_exist = result.score.first().copied().map_or(false, |score| score > 0.8);
        if is_exist {
            if let Some(ValueVec::String(event_name)) = result.field.first().map(|c| &c.value) {
                if let Some(exist_event) = event_name.get(0).cloned() {
                    commit_hotspot(&message.namespace, &exist_event).await?;
                    tracing::info!("[handle_search_report] commit_hotspot exist event = {}", exist_event);
                    return Ok(());
                }
            }
        }
    }

    store_event_embedding(&event, embedding).await
        .context("[handle_search_report] store_event_embedding err.")?;
    commit_hotspot(&message.namespace, &event).await?;
    tracing::info!("[handle_search_report] commit_hotspot new event = {}", event);
    Ok(())
}