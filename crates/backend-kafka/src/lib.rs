//! Kafka — the durability/replay standard, measured over the official C
//! client (librdkafka, vendored and built by rdkafka's cmake-build).
//!
//! Configuration, for the report — this is the documented one tuning
//! (REQUIREMENTS.md allows default + one), and it is the standard Kafka
//! latency configuration rather than anything exotic:
//! - producer: `linger.ms=0` (librdkafka's default is 5ms of deliberate
//!   batching delay, which would swamp every number), `acks=all`.
//! - consumer: `fetch.wait.max.ms=5`, `fetch.error.backoff.ms=10`, unique
//!   throwaway group, `auto.offset.reset=earliest` so a consumer that
//!   finishes joining after the first sends replays instead of losing them.
//! - broker (set by the runner): single node KRaft, one partition,
//!   auto-create topics on.
//!
//! `send` enqueues into librdkafka's async queue and returns — Kafka has no
//! synchronous fire-and-forget — so delivery failures surface as drops in
//! the harness accounting rather than as send errors. A full queue busy-
//! polls until space opens, which surfaces as send lag, the honest place.

use std::io;
use std::time::{Duration, Instant};

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{BaseProducer, BaseRecord};
use rdkafka::Message;

use harness::transport::{Receiver, RecvOutcome, Sender};

const RECV_TIMEOUT: Duration = Duration::from_secs(1);

pub fn default_bind(port: u16) -> String {
    format!("127.0.0.1:{port}")
}

fn err(e: impl std::fmt::Display) -> io::Error {
    io::Error::other(format!("kafka: {e}"))
}

fn split_addr(addr: &str) -> (&str, &str) {
    match addr.split_once('/') {
        Some((hp, base)) => (hp, base),
        None => (addr, "wg"),
    }
}

fn producer(hp: &str) -> io::Result<BaseProducer> {
    ClientConfig::new()
        .set("bootstrap.servers", hp)
        .set("linger.ms", "0")
        .set("acks", "all")
        .create()
        .map_err(err)
}

fn consumer(hp: &str, topic: &str) -> io::Result<BaseConsumer> {
    let c: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", hp)
        .set("group.id", format!("wg-{}", std::process::id()))
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("allow.auto.create.topics", "true")
        .set("fetch.wait.max.ms", "5")
        .set("fetch.error.backoff.ms", "10")
        .create()
        .map_err(err)?;
    c.subscribe(&[topic]).map_err(err)?;

    // Group join + topic auto-creation take seconds on a cold broker, and a
    // schedule that starts before assignment records the join as transport
    // latency (or, worse, gives up during drain and calls everything
    // dropped). Poll until this consumer actually owns the partition; no
    // messages can predate assignment here, because both ends do this wait
    // before the first message of a run is ever published.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match c.poll(Duration::from_millis(100)) {
            Some(Err(e)) => return Err(err(e)),
            Some(Ok(_)) => {
                return Err(err("received a message before the run started"));
            }
            None => {}
        }
        let assigned = c.assignment().map_err(err)?.count() > 0;
        if assigned {
            return Ok(c);
        }
        if Instant::now() > deadline {
            return Err(err(format!(
                "no partition assignment for {topic} after 30s"
            )));
        }
    }
}

/// Kafka 4 does not auto-create a topic for a mere consumer subscription,
/// and a subscription to a nonexistent topic waits forever for partitions —
/// so the echo side creates both topics explicitly before anything
/// subscribes. AlreadyExists is fine (a retried run).
fn create_topics(hp: &str, topics: [&str; 2]) -> io::Result<()> {
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    let admin: AdminClient<_> = ClientConfig::new()
        .set("bootstrap.servers", hp)
        .create()
        .map_err(err)?;
    let new: Vec<NewTopic> = topics
        .iter()
        .map(|t| NewTopic::new(t, 1, TopicReplication::Fixed(1)))
        .collect();
    let results = futures::executor::block_on(admin.create_topics(&new, &AdminOptions::new()))
        .map_err(err)?;
    for r in results {
        match r {
            Ok(_) => {}
            Err((_, rdkafka::types::RDKafkaErrorCode::TopicAlreadyExists)) => {}
            Err((topic, code)) => return Err(err(format!("create {topic}: {code}"))),
        }
    }
    Ok(())
}

pub fn echo(bind: &str, _msg_size: usize) -> io::Result<()> {
    let base = format!("wg{}", std::process::id());
    create_topics(bind, [&format!("{base}-c2s"), &format!("{base}-s2c")])?;
    let rx = consumer(bind, &format!("{base}-c2s"))?;
    let tx = producer(bind)?;
    let out = format!("{base}-s2c");

    println!("READY {bind}/{base}");
    use std::io::Write;
    io::stdout().flush()?;

    loop {
        match rx.poll(RECV_TIMEOUT) {
            Some(Ok(msg)) => {
                let payload = msg.payload().unwrap_or(&[]);
                enqueue(&tx, &out, payload)?;
                tx.poll(Duration::ZERO);
            }
            Some(Err(e)) => return Err(err(e)),
            None => {} // quiet second; keep serving until killed
        }
    }
}

pub fn connect(addr: &str, _msg_size: usize) -> io::Result<(Box<dyn Sender>, Box<dyn Receiver>)> {
    let (hp, base) = split_addr(addr);
    let tx = KafkaSender {
        producer: producer(hp)?,
        topic: format!("{base}-c2s"),
    };
    let rx = KafkaReceiver {
        consumer: consumer(hp, &format!("{base}-s2c"))?,
    };
    Ok((Box::new(tx), Box::new(rx)))
}

fn enqueue(producer: &BaseProducer, topic: &str, payload: &[u8]) -> io::Result<()> {
    let mut record = BaseRecord::<(), [u8]>::to(topic).payload(payload);
    loop {
        match producer.send(record) {
            Ok(()) => return Ok(()),
            Err((e, rec))
                if e.rdkafka_error_code() == Some(rdkafka::types::RDKafkaErrorCode::QueueFull) =>
            {
                producer.poll(Duration::from_millis(1));
                record = rec;
            }
            Err((e, _)) => return Err(err(e)),
        }
    }
}

struct KafkaSender {
    producer: BaseProducer,
    topic: String,
}

impl Sender for KafkaSender {
    fn send(&mut self, msg: &[u8]) -> io::Result<()> {
        enqueue(&self.producer, &self.topic, msg)?;
        // Serve delivery callbacks without blocking; actual writes are
        // librdkafka's own I/O thread's job.
        self.producer.poll(Duration::ZERO);
        Ok(())
    }
}

struct KafkaReceiver {
    consumer: BaseConsumer,
}

impl Receiver for KafkaReceiver {
    fn recv(&mut self, buf: &mut [u8]) -> io::Result<RecvOutcome> {
        match self.consumer.poll(RECV_TIMEOUT) {
            Some(Ok(msg)) => {
                let payload = msg.payload().unwrap_or(&[]);
                if payload.len() != buf.len() {
                    return Err(err(format!(
                        "message of {} bytes, expected {}",
                        payload.len(),
                        buf.len()
                    )));
                }
                buf.copy_from_slice(payload);
                Ok(RecvOutcome::Msg)
            }
            Some(Err(e)) => Err(err(e)),
            None => Ok(RecvOutcome::TimedOut),
        }
    }
}
