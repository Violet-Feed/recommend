use std::collections::HashMap;
use milvus::client::Client;
use milvus::collection::SearchOption;
use milvus::data::FieldColumn;
use milvus::index::{IndexParams, IndexType};
use milvus::schema::{CollectionSchemaBuilder, FieldSchema};
use rand::Rng;

pub async fn init() ->Result<(), Box<dyn std::error::Error>> {
    const URL: &str = "http://localhost:19530";

    let client = Client::new(URL).await?;

    let schema =
        CollectionSchemaBuilder::new("hello_milvus", "a guide example for milvus rust SDK")
            .add_field(FieldSchema::new_primary_int64(
                "id",
                "primary key field",
                true,
            ))
            .add_field(FieldSchema::new_float_vector(
                "embedding",
                "feature field",
                1024,
            ))
            .build()?;
    let collection = client.create_collection(schema.clone(), None).await?;

    // if let Err(err) = hello_milvus(&client, &schema).await {
    //     println!("failed to run hello milvus: {:?}", err);
    // }
    if let Err(err) = hello_milvus(&client, "hello_milvus").await {
        println!("failed to run hello milvus: {:?}", err);
    }
    collection.drop().await?;

    Ok(())
}

// async fn hello_milvus(client: &Client, collection: &CollectionSchema) -> Result<(), Box<dyn std::error::Error>> {
//     let mut embed_data = Vec::<f32>::new();
//     for _ in 1..=256 * 1000 {
//         let mut rng = rand::thread_rng();
//         let embed = rng.r#gen();
//         embed_data.push(embed);
//     }
//     let embed_column =
//         FieldColumn::new(collection.get_field("embedding").unwrap(), embed_data);
//
//     client.insert(collection.name(), vec![embed_column], None).await?;
//     client.flush(collection.name()).await?;
//     let index_params = IndexParams::new(
//         "feature_index".to_owned(),
//         IndexType::IvfFlat,
//         milvus::index::MetricType::L2,
//         HashMap::from([("nlist".to_owned(), "32".to_owned())]),
//     );
//     client.create_index(collection.name(), "embedding", index_params).await?;
//     client.load_collection(collection.name(), Some(LoadOptions::default())).await?;
//
//     let options = QueryOptions::default();
//     let result = client.query(collection.name(), "id > 0", &options).await?;
//
//     tracing::info!("result num: {}",result.first().map(|c| c.len()).unwrap_or(0),);
//
//     Ok(())
// }

async fn hello_milvus(client: &Client, collection_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut embed_data = Vec::<f32>::new();
    let mut rng = rand::rng();
    for _ in 1..=1024 * 1000 {
        let embed = rng.random();
        embed_data.push(embed);
    }
    let collection = client.get_collection(collection_name).await?;
    let embed_column =
        FieldColumn::new(collection.schema().get_field("embedding").unwrap(), embed_data.clone());

    collection.insert(vec![embed_column], None).await?;
    collection.flush().await?;

    let index_params = IndexParams::new(
        "feature_index".to_owned(),
        IndexType::IvfFlat,
        milvus::index::MetricType::L2,
        HashMap::from([("nlist".to_owned(), "32".to_owned())]),
    );
    collection.create_index("embedding", index_params).await?;
    collection.load(1).await?;

    let indexes = collection
        .describe_index("embedding")
        .await?;
    let index= indexes.first().unwrap();

    let options = SearchOption::default();
    let results = collection.search::<&str, Vec<_>>(vec![embed_data.into()],"embedding",10,index.params().metric_type(),vec!["test"], &options).await?;

    for result in &results {
        tracing::info!("result: {:?}", result.size);
    }
    Ok(())
}