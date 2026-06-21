use anyhow::{Context, Result};
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    consumer::{BaseConsumer, Consumer},
    producer::{FutureProducer, FutureRecord},
    util::Timeout,
    ClientConfig, Message, Offset, TopicPartitionList,
};
use std::time::Duration;

const PROBE_TOPIC: &str = "antithesis-probe";
const PROBE_PARTITION: i32 = 0;

pub struct DurabilityProbe {
    bootstrap_servers: String,
}

impl DurabilityProbe {
    pub fn new(bootstrap_servers: &str) -> Self {
        Self {
            bootstrap_servers: bootstrap_servers.to_string(),
        }
    }

    pub async fn run(&self) -> Result<(bool, String, i32, i64)> {
        self.ensure_topic_exists().await?;

        let (probe_id, partition, offset) = self.produce_probe_message().await?;
        println!(
            "probe produced: id={} partition={} offset={}",
            probe_id, partition, offset
        );

        let found = self.verify_probe_message(&probe_id, partition, offset)?;
        if found {
            println!(
                "probe confirmed durable: partition={} offset={}",
                partition, offset
            );
        }

        Ok((found, probe_id, partition, offset))
    }

    async fn ensure_topic_exists(&self) -> Result<()> {
        let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", &self.bootstrap_servers)
            .create()
            .context("failed to create admin client")?;

        let new_topic = NewTopic::new(PROBE_TOPIC, 1, TopicReplication::Fixed(3))
            .set("min.insync.replicas", "3");
        // Ignore errors — topic likely already exists from a prior invocation
        let _ = admin
            .create_topics(&[new_topic], &AdminOptions::new())
            .await;

        Ok(())
    }

    async fn produce_probe_message(&self) -> Result<(String, i32, i64)> {
        let probe_id = uuid::Uuid::new_v4().to_string();

        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &self.bootstrap_servers)
            .set("request.required.acks", "all")
            .set("enable.idempotence", "true")
            .set("message.timeout.ms", "30000")
            .create()
            .context("failed to create producer")?;

        let record = FutureRecord::<str, String>::to(PROBE_TOPIC)
            .payload(&probe_id)
            .partition(PROBE_PARTITION);

        let (partition, offset) = producer
            .send(record, Timeout::After(Duration::from_secs(30)))
            .await
            .map_err(|(e, _)| anyhow::anyhow!("probe produce failed: {}", e))?;

        Ok((probe_id, partition, offset))
    }

    fn verify_probe_message(&self, probe_id: &str, partition: i32, offset: i64) -> Result<bool> {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &self.bootstrap_servers)
            .set("group.id", format!("antithesis-probe-{}", probe_id))
            .set("auto.offset.reset", "earliest")
            .create()
            .context("failed to create consumer")?;

        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(PROBE_TOPIC, partition, Offset::Offset(offset))
            .context("failed to set partition offset")?;
        consumer.assign(&tpl).context("failed to assign partition")?;

        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        let mut found = false;

        while std::time::Instant::now() < deadline {
            match consumer.poll(Duration::from_secs(5)) {
                Some(Ok(msg)) => {
                    if let Some(Ok(payload)) = msg.payload_view::<str>() {
                        if payload == probe_id {
                            found = true;
                            break;
                        }
                    }
                }
                Some(Err(e)) => println!("consumer poll error: {}", e),
                None => {}
            }
        }

        Ok(found)
    }
}
