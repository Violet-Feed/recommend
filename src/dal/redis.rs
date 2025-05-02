use r2d2::Pool;
use redis::{Client, Commands};
use tokio::sync::OnceCell;
use anyhow::{Context, Result};
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
    let history: Vec<(String, f64)> = con.zrevrange_withscores(key, 0, 4)?;
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