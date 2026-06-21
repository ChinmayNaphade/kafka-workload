use anyhow::{Context, Result};
use antithesis_kafka_workload::{
    config::WorkloadConfig,
    validation::durability_probe::DurabilityProbe,
};
use serde_json::json;
use std::env;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    antithesis_sdk::antithesis_init();

    let args: Vec<String> = env::args().collect();
    let config_path = args
        .get(1)
        .map(|s| s.as_str())
        .unwrap_or("/app/workload-config.json");
    let config = WorkloadConfig::new(config_path).context("failed to read configuration file")?;

    let probe = DurabilityProbe::new(&config.bootstrap_servers);
    let (found, probe_id, partition, offset) = probe.run().await?;

    antithesis_sdk::assert_always!(
        found,
        "Probe message must be durably readable after acknowledged write",
        &json!({
            "probe_id": probe_id,
            "topic": "antithesis-probe",
            "partition": partition,
            "offset": offset,
            "bootstrap_servers": config.bootstrap_servers,
        })
    );

    Ok(())
}
