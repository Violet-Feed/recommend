use crate::dal::{milvus, model};
use crate::recommend::{EmbeddingReport};
use anyhow::{Context, Result};

pub async fn handle_embedding_report(namespace:&str, report: EmbeddingReport) -> Result<()> {
    match namespace {
        "item" => {
            let embedding = model::call_multi_embedding_model(&report.texts, &report.images,&report.videos).await
                .context("[handle_embedding_report] call_multi_embedding_model err.")?;
            milvus::upsert_item(&report.extra, embedding).await
                .context("[handle_embedding_report] upsert_item err.")?;
        }
        _ => {
            tracing::error!("[handle_embedding_report] unknown namespace: {}", namespace);
        }
    }
    Ok(())
}
