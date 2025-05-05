use r2d2::Pool;
use redis::{Client, Commands, SetOptions};
use tokio::sync::OnceCell;
use anyhow::{Context, Result};
use redis::SetExpiry::EX;
use serde_json::Value;

static REDIS_CLIENT: OnceCell<Pool<Client>> = OnceCell::const_new();

async fn get_redis_client() -> &'static Pool<Client> {
    REDIS_CLIENT.get_or_init(|| async {
        let client = Client::open("redis://127.0.0.1/").expect("Failed to create redis client");
        let pool = Pool::builder().build(client).expect("Failed to create redis connection pool");
        pool
    }).await
}

pub async fn get_user_history(user_id:i64) -> Result<Vec<i64>> {
    let mut con = get_redis_client().await.get()
        .context("[get_user_history] Failed to get redis client")?;
    let key = format!("item_history:{}", user_id);
    let history: Vec<(String, f64)> = con.zrevrange_withscores(key, 0, 4)
        .context("[get_user_history] redis zrevrange_withscores err.")?;
    let history_id: Vec<i64> = history
        .iter()
        .filter_map(|(id, _)| id.parse::<i64>().ok())
        .collect();
    Ok(history_id)
}

pub async fn write_impression(user_id:i64,items:Vec<Value>) -> Result<()> {
    let mut con = get_redis_client().await.get()
        .context("[write_impression] Failed to get redis client")?;
    let key = format!("item_impression:{}", user_id);
    let mut cmd = redis::cmd("BF.INSERT");
    cmd.arg(key)
        .arg("CAPACITY")
        .arg(1000);
    for item in items.iter() {
        cmd.arg(item.get("item_id").unwrap().as_i64().unwrap());
    }
    let _: Vec<bool> =cmd.query(&mut con)
        .context("[write_impression] redis BF.INSERT err.")?;
    Ok(())
}

pub async fn execute_impression(user_id:i64,items:Vec<Value>) -> Result<Vec<Value>> {
    let mut con = get_redis_client().await.get()
        .context("[execute_impression] Failed to get redis client")?;
    let key = format!("item_impression:{}", user_id);
    let mut cmd = redis::cmd("BF.MEXISTS");
    cmd.arg(key);
    for item in items.iter() {
        cmd.arg(item.get("item_id").unwrap().as_i64().unwrap());
    }
    let exists: Vec<bool> = cmd.query(&mut con)
        .context("[execute_impression] redis BF.MEXISTS err.")?;
    let mut filter_items = Vec::new();
    for (item, exist) in items.into_iter().zip(exists) {
        if !exist {
            filter_items.push(item);
        }
    }
    Ok(filter_items)
}

pub async fn set_hotspot(namespace:&str, key:&str) -> Result<()> {
    let mut con = get_redis_client().await.get()
        .context("[set_hotspot] Failed to get redis client")?;
    let key = format!("hotspot:{}:{}", namespace,key);
    let _: () = con.set_options(key, 1, SetOptions::default().with_expiration(EX(3600)))
        .context("[set_hotspot] redis set err.")?;
    Ok(())
}

pub async fn set_topk(namespace:&str,topk:&str) -> Result<()> {
    let mut con = get_redis_client().await.get()
        .context("[set_topk] Failed to get redis client")?;
    let key = format!("topk:{}", namespace);
    let _: () = con.set(key, topk)
        .context("[set_topk] redis set err.")?;
    Ok(())
}