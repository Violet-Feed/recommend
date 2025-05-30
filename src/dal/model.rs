use anyhow::{Context, Result};
use serde_json::{json, Value};
use std::env;

pub async fn call_multi_embedding_model(texts: &Vec<String>, images: &Vec<String>, videos:&Vec<String>) -> Result<Vec<f32>> {
    let api_key = env::var("DASHSCOPE_API_KEY")?;
    let client = reqwest::Client::new();

    let mut contents = Vec::new();

    for text in texts {
        contents.push(json!({"text": text}));
    }
    for image in images {
        contents.push(json!({"image": image}));
    }
    for video in videos {
        contents.push(json!({"video": video}));
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
        .await
        .context("[call_multi_embedding_model] send request err.")?;

    let result = response.json::<Value>().await
        .context("[call_multi_embedding_model] resp parse json err.")?;
    let embedding: Vec<f32> = serde_json::from_value(result["output"]["embeddings"][0]["embedding"].clone())?;
    Ok(embedding)
}

pub async fn call_text_embedding_model(event: &str) -> Result<Vec<f32>> {
    let api_key = env::var("DASHSCOPE_API_KEY")?;
    let client = reqwest::Client::new();

    let request_body = json!({
        "model": "text-embedding-v3",
        "input": {
            "texts": [
                format!("{}", event)
            ]
        },
        "parameters": {
            "dimension": 1024
        }
    });

    let response = client
        .post("https://dashscope.aliyuncs.com/api/v1/services/embeddings/text-embedding/text-embedding")
        .header("Authorization", format!("Bearer {}", api_key))
        .header("Content-Type", "application/json")
        .json(&request_body)
        .send()
        .await
        .context("[call_text_embedding_model] send request err.")?;

    let result = response.json::<Value>().await
        .context("[call_text_embedding_model] resp parse json err.")?;
    let embedding: Vec<f32> = serde_json::from_value(result["output"]["embeddings"][0]["embedding"].clone())?;
    Ok(embedding)
}

pub async fn call_event_model(keyword: &str) -> Result<String> {
    let api_key = env::var("DASHSCOPE_API_KEY")?;
    let client = reqwest::Client::new();
    let prompt = "这下面是用户的搜索词，我希望你将搜索词提炼成一个热点词，要求不超过十个字，如果搜索词无意义请输出null，我希望你只输出热点词内容：\n";
    let request_body = json!({
        //"model": "qwen2.5-1.5b-instruct",
        "model": "qwen-turbo",
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
        .await
        .context("[call_event_model] send request err.")?;

    let result = response.json::<Value>().await
        .context("[call_event_model] resp parse json err.")?;
    let content = result["choices"][0]["message"]["content"].as_str().unwrap_or("null");
    let event = if content.len() >= 2 && content.starts_with('"') && content.ends_with('"') {
        &content[1..content.len() - 1]
    } else {
        content
    };
    Ok(event.to_string())
}
