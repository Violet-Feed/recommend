use crate::recommend::HotSpotReport;
use anyhow::{Context, Result};
use crate::hotspot;

pub async fn handle_hotspot_report(namespace:&str, report: HotSpotReport) -> Result<()> {
    hotspot::detect_hotspot(namespace, &report.key).await
        .context("[handle_hotspot_report] detect_hotspot err.")?;
    Ok(())
}