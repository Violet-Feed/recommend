use anyhow::Result;
use milvus::client::Client;
use milvus::collection::{Collection, SearchOption, SearchResult};
use milvus::data::FieldColumn;
use milvus::index::MetricType::IP;
use tokio::sync::OnceCell;

const EVENT_COLLECTION_NAME: &str = "event";
static EVENT_COLLECTION_CLIENT: OnceCell<Collection> = OnceCell::const_new();

async fn get_event_collection() -> &'static Collection {
    EVENT_COLLECTION_CLIENT.get_or_init(|| async {
            let client = Client::new("http://localhost:19530").await.expect("Failed to initialize milvus client");
            let collection = client.get_collection(EVENT_COLLECTION_NAME).await.expect("Failed to get collection");
            collection.load(1).await.expect("Failed to load collection");
            collection
        }).await
}

pub async fn store_event_embedding(event_name: &str, embedding_data: Vec<f32>) -> Result<()> {
    let collection = get_event_collection().await;
    let event_name_column = FieldColumn::new(collection.schema().get_field("event_name").unwrap(), vec![event_name.to_string()], );
    let event_embedding_column = FieldColumn::new(collection.schema().get_field("event_embedding").unwrap(), embedding_data, );
    collection.insert(vec![event_embedding_column, event_name_column], None).await?;
    collection.flush().await?;
    Ok(())
}

pub async fn search_event_embedding<'a>(embedding: Vec<f32>) -> Result<Option<SearchResult<'a>>> {
    let collection = get_event_collection().await;
    let options = SearchOption::default();
    let results = collection.search(vec![embedding.into()], "event_embedding", 1, IP, vec!["event_name"], &options, ).await?;
    Ok(results.into_iter().next())
}