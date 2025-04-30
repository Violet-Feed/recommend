use crate::dal::model::call_embedding_model;
use crate::recommend::ReportMessage;
//use crate::dal::milvus::{store_event_embedding, search_event_embedding};
use anyhow::{Context, Result};

pub async fn handle_embedding_report(message: ReportMessage) -> Result<()> {
    let embedding = call_embedding_model(&message.texts, &message.images).await
        .context("[handle_embedding_report] call_embedding_model err.")?;

    //store_event_embedding("event",&embedding).await.context("[handle_embedding_report] store_embedding err.")?;
    //let results =search_event_embedding("event",&embedding).await.context("[handle_embedding_report] search_embedding err.")?;
    //tracing::info!("[handle_embedding_report] search_embedding. results = {:?}", results.first().unwrap().size);
    //if results.first().is_some_and(results.first().)
    Ok(())
}
