use std::env;
use serde_json::{json, Value};
pub async fn call_embedding_model(
    texts: &[String],
    images: &[String]
) -> Result<Value, Box<dyn std::error::Error>> {
    let api_key = env::var("DASHSCOPE_API_KEY")?;
    let client = reqwest::Client::new();
    
    let mut contents = Vec::new();
    
    for text in texts {
        contents.push(json!({"text": text}));
    }
    
    for image in images {
        contents.push(json!({"image": image}));
    }

    let request_body = json!({
        "model": "multimodal-embedding-v1",
        "input": {
            "contents": contents
        },
        "parameters": {}
    });
    
    let response = client
        .post("https://dashscope.aliyuncs.com/api/v1/services/embeddings/multimodal-embedding/multimodal-embedding")
        .header("Authorization", format!("Bearer {}", api_key))
        .header("Content-Type", "application/json")
        .json(&request_body)
        .send()
        .await?;
    
    let result = response.json::<Value>().await?;

    Ok(result)
}

pub async fn call_event_model(
    keyword: &str
) -> Result<Value, Box<dyn std::error::Error>> {
    let api_key = env::var("DASHSCOPE_API_KEY")?;
    let client = reqwest::Client::new();
    let prompt="这下面是用户的搜索词，我希望你将搜索词提炼成一个热点词，要求不超过十个字，如果搜索词无意义请输出null，我希望你只输出热点词内容：\n";
    let request_body = json!({
        "model": "qwen2.5-1.5b-instruct",
        "messages": [
            {
                "role": "user",
                "content": format!("{}{}",prompt,keyword)
            }
        ]
    });

    let response = client
        .post("https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions")
        .header("Authorization", format!("Bearer {}", api_key))
        .header("Content-Type", "application/json")
        .json(&request_body)
        .send()
        .await?;

    let result = response.json::<Value>().await?;
    let content = result["choices"][0]["message"]["content"].as_str().unwrap_or("null");
    let event = if content.len() >= 2 && content.starts_with('"') && content.ends_with('"') {
        &content[1..content.len() - 1]
    } else {
        content
    };
    Ok(event.into())
}
