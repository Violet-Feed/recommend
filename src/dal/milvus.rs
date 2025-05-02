use anyhow::{Context, Result};
use milvus::client::Client;
use milvus::collection::{Collection, SearchOption, SearchResult};
use milvus::data::FieldColumn;
use milvus::index::MetricType::IP;
use rand::Rng;
use serde_json::{json, Value};
use tokio::sync::OnceCell;

static EVENT_COLLECTION_CLIENT: OnceCell<Collection> = OnceCell::const_new();

async fn get_event_collection() -> &'static Collection {
    EVENT_COLLECTION_CLIENT.get_or_init(|| async {
        let client = Client::new("http://localhost:19530").await.expect("Failed to initialize milvus client");
        let collection = client.get_collection("event").await.expect("Failed to get event collection");
        collection.load(1).await.expect("Failed to load event collection");
        collection
    }).await
}

pub fn random_embedding() -> Vec<f32> {
    let mut rng = rand::thread_rng();
    (0..1024).map(|_| rng.gen_range(-0.05..0.05)).collect()
}

pub async fn insert_event(event_name: &str, embedding_data: Vec<f32>) -> Result<()> {
    let collection = get_event_collection().await;
    let event_name_column = FieldColumn::new(collection.schema().get_field("event_name").unwrap(), vec![event_name.to_string()], );
    let event_embedding_column = FieldColumn::new(collection.schema().get_field("event_embedding").unwrap(), embedding_data);
    collection.insert(vec![event_embedding_column, event_name_column], None).await?;
    collection.flush().await?;
    Ok(())
}
pub async fn recall_event<'a>(embedding: Vec<f32>) -> Result<Option<SearchResult<'a>>> {
    let collection = get_event_collection().await;
    let options = SearchOption::default();
    let results = collection.search(vec![embedding.into()], "event_embedding", 1, IP, vec!["event_name"], &options, ).await?;
    Ok(results.into_iter().next())
}

pub async fn upsert_item(extra: &str, embedding_data: Vec<f32>) -> Result<()> {
    let mut extra_obj: Value = serde_json::from_str(extra)
        .context("[upsert_item] extra deserializing json err.")?;
    let obj = extra_obj.as_object_mut()
        .context("[upsert_item] extra deserializing json err.")?;
    obj.insert("multi_embedding".to_string(), json!(embedding_data));

    let data = vec![Value::Object(obj.to_owned())];
    let body = json!({
        "data": data,
        "collectionName": "item"
    });
    let client = reqwest::Client::new();
    let resp = client
        .post("http://localhost:19530/v2/vectordb/entities/upsert")
        .json(&body)
        .send()
        .await
        .context("[upsert_item] send request err.")?;
    let text = resp.text().await
        .context("[upsert_item] get response err.")?;
    tracing::info!("[upsert_item] {}", text);
    Ok(())
}
pub async fn get_item_embedding(ids: Vec<i64>) -> Result<Vec<Vec<f32>>> {
    let body = json!({
        "collectionName": "item",
        "id": ids,
        "outputFields": ["multi_embedding"]
    });

    let client = reqwest::Client::new();
    let resp = client
        .post("http://localhost:19530/v2/vectordb/entities/get")
        .json(&body)
        .send()
        .await
        .context("[get_item_embedding] send request err.")?;

    let text = resp.text().await
        .context("[get_item_embedding] get response err.")?;
    let v: Value = serde_json::from_str(&text)
        .context("[get_item_embedding] parse json err.")?;

    let data = v.get("data")
        .and_then(|d| d.as_array())
        .ok_or_else(|| anyhow::anyhow!("no data field"))?;

    let mut embeddings = Vec::new();
    for obj in data {
        if let Some(embedding) = obj.get("multi_embedding").and_then(|e| e.as_array()) {
            let vec: Vec<f32> = embedding.iter()
                .filter_map(|x| x.as_f64().map(|f| f as f32))
                .collect();
            embeddings.push(vec);
        }
    }
    Ok(embeddings)
}
pub async fn recall_item(embeddings: Vec<Vec<f32>>,step:i64) -> Result<Vec<Value>> {
    let limits = [15, 12, 9, 6, 3, 5];
    let req: Vec<Value> = embeddings
        .into_iter()
        .zip(limits.iter())
        .map(|(embedding, &limit)| {
            json!({
                "data": [embedding],
                "annsField": "multi_embedding",
                "params": {
                    "params": {
                        "ef": 10
                    }
                },
                "offset": (step - 1) * limit,
                "limit": limit
            })
        })
        .collect();
    let body = json!({
        "collectionName": "item",
        "search": req,
        "rerank": {
            "strategy": "rrf",
            "params": {
                "k": 60
            }
        },
        "limit": 50,
        "outputFields": [
            "item_id",
            "title",
            "image"
        ]
    });

    let client = reqwest::Client::new();
    let resp = client
        .post("http://localhost:19530/v2/vectordb/entities/advanced_search")
        .json(&body)
        .send()
        .await
        .context("[recall_item] send request err.")?;
    let text = resp.text().await
        .context("[recall_item] get response err.")?;
    tracing::info!("[recall_item] {}", text);

    let mut v: Value = serde_json::from_str(&text)
        .context("[recall_item] serialize json err.")?;
    let data = v.get_mut("data")
        .and_then(|d| d.as_array_mut())
        .ok_or_else(|| anyhow::anyhow!("no data field"))?;
    for obj in data.iter_mut() {
        if let Some(map) = obj.as_object_mut() {
            map.remove("distance");
            map.remove("id");
        }
    }
    Ok(data.to_owned())
}
pub async fn search_item(embedding: Vec<f32>, keyword: &str, page: i64) -> Result<Vec<Value>> {
    let req = json!([
        {
            "data": [embedding],
            "annsField": "multi_embedding",
            "params": {
                "params": {
                    "ef": 10
                }
            },
            "offset": (page - 1) * 10,
            "limit": 10
        },
        {
            "data": [keyword],
            "annsField": "title_embeddings",
            "params": {
                "params": {
                    "drop_ratio_build": 0.2
                }
            },
            "offset": (page - 1) * 10,
            "limit": 10
        }
    ]);
    let body = json!({
        "collectionName": "item",
        "search": req,
        "rerank": {
            "strategy": "rrf",
            "params": {
                "k": 60
            }
        },
        "limit": 20,
        "outputFields": [
            "item_id",
            "title",
            "image"
        ]
    });

    let client = reqwest::Client::new();
    let resp = client
        .post("http://localhost:19530/v2/vectordb/entities/advanced_search")
        .json(&body)
        .send()
        .await
        .context("[search_item] send request err.")?;
    let text = resp.text().await
        .context("[search_item] get response err.")?;
    tracing::info!("[search_item] {}", text);

    let mut v: Value = serde_json::from_str(&text)
        .context("[search_item] serialize json err.")?;
    let data = v.get_mut("data")
        .and_then(|d| d.as_array_mut())
        .ok_or_else(|| anyhow::anyhow!("no data field"))?;
    for obj in data.iter_mut() {
        if let Some(map) = obj.as_object_mut() {
            map.remove("distance");
            map.remove("id");
        }
    }
    Ok(data.to_owned())
}