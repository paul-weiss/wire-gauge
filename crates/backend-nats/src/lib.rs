//! NATS — two backends in one crate, deliberately counted as separate
//! candidates (REQUIREMENTS.md):
//!
//! - `nats` — core NATS: brokered fire-and-forget pub/sub. No persistence,
//!   no acks; a slow subscriber is cut off by the server, and any loss
//!   shows up in the harness's drop accounting.
//! - `jetstream` — persisted delivery. Both directions run through
//!   file-backed streams; the publisher core-publishes onto a subject the
//!   stream captures, and the consumer is an ordered pull consumer. What
//!   this measures is *persisted delivery latency* — publish → stored →
//!   delivered — not publisher ack latency, which would serialize the
//!   send schedule (an honest ack-throughput scenario is future work).
//!
//! The official client is async (`async-nats`), so this backend carries a
//! one-worker tokio runtime and pays a `block_on` boundary per operation —
//! exactly what a blocking Rust caller of the official client pays, and the
//! report says so. Publishes are followed by an explicit `flush()` so a
//! message is on the wire before `send` returns; without it the client may
//! buffer.

use std::io;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use tokio::runtime::Runtime;

use harness::transport::{Receiver, RecvOutcome, Sender};

const RECV_TIMEOUT: Duration = Duration::from_secs(1);

pub fn default_bind(port: u16) -> String {
    format!("127.0.0.1:{port}")
}

fn err(e: impl std::fmt::Display) -> io::Error {
    io::Error::other(format!("nats: {e}"))
}

fn runtime() -> io::Result<Arc<Runtime>> {
    Ok(Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()?,
    ))
}

fn split_addr(addr: &str) -> (&str, &str) {
    match addr.split_once('/') {
        Some((hp, base)) => (hp, base),
        None => (addr, "wg"),
    }
}

async fn connect_client(hp: &str) -> io::Result<async_nats::Client> {
    async_nats::connect(hp).await.map_err(err)
}

// ---------------------------------------------------------------- core NATS

pub fn echo(bind: &str, _msg_size: usize) -> io::Result<()> {
    let base = format!("wg{}", std::process::id());
    let rt = runtime()?;
    rt.block_on(async {
        let client = connect_client(bind).await?;
        let mut sub = client.subscribe(format!("{base}.c2s")).await.map_err(err)?;
        let out = format!("{base}.s2c");

        println!("READY {bind}/{base}");
        use std::io::Write;
        io::stdout().flush()?;

        while let Some(msg) = sub.next().await {
            client
                .publish(out.clone(), msg.payload)
                .await
                .map_err(err)?;
            client.flush().await.map_err(err)?;
        }
        Ok(())
    })
}

pub fn connect(addr: &str, _msg_size: usize) -> io::Result<(Box<dyn Sender>, Box<dyn Receiver>)> {
    let (hp, base) = split_addr(addr);
    let rt = runtime()?;
    let (client, sub) = rt.block_on(async {
        let client = connect_client(hp).await?;
        let sub = client.subscribe(format!("{base}.s2c")).await.map_err(err)?;
        // Server processes the SUB before anything we later publish.
        client.flush().await.map_err(err)?;
        Ok::<_, io::Error>((client, sub))
    })?;
    let tx = NatsSender {
        rt: Arc::clone(&rt),
        client,
        subject: format!("{base}.c2s"),
    };
    let rx = NatsReceiver { rt, sub: Some(sub) };
    Ok((Box::new(tx), Box::new(rx)))
}

struct NatsSender {
    rt: Arc<Runtime>,
    client: async_nats::Client,
    subject: String,
}

impl Sender for NatsSender {
    fn send(&mut self, msg: &[u8]) -> io::Result<()> {
        self.rt.block_on(async {
            self.client
                .publish(self.subject.clone(), msg.to_vec().into())
                .await
                .map_err(err)?;
            self.client.flush().await.map_err(err)
        })
    }
}

struct NatsReceiver {
    rt: Arc<Runtime>,
    /// Option so Drop can hand it back to the runtime: Subscriber's Drop
    /// calls tokio::spawn and panics outside a runtime context.
    sub: Option<async_nats::Subscriber>,
}

impl Drop for NatsReceiver {
    fn drop(&mut self) {
        let _guard = self.rt.enter();
        drop(self.sub.take());
    }
}

impl Receiver for NatsReceiver {
    fn recv(&mut self, buf: &mut [u8]) -> io::Result<RecvOutcome> {
        let sub = self.sub.as_mut().expect("present until drop");
        let msg = self
            .rt
            .block_on(async { tokio::time::timeout(RECV_TIMEOUT, sub.next()).await });
        match msg {
            Ok(Some(m)) => {
                if m.payload.len() != buf.len() {
                    return Err(err(format!(
                        "message of {} bytes, expected {}",
                        m.payload.len(),
                        buf.len()
                    )));
                }
                buf.copy_from_slice(&m.payload);
                Ok(RecvOutcome::Msg)
            }
            Ok(None) => Ok(RecvOutcome::Closed),
            Err(_elapsed) => Ok(RecvOutcome::TimedOut),
        }
    }
}

// ---------------------------------------------------------------- JetStream

mod js {
    use super::*;
    use async_nats::jetstream;

    pub async fn stream_and_consumer(
        context: &jetstream::Context,
        stream_name: &str,
        subject: &str,
    ) -> io::Result<jetstream::consumer::pull::Ordered> {
        let stream = context
            .get_or_create_stream(jetstream::stream::Config {
                name: stream_name.to_string(),
                subjects: vec![subject.to_string()],
                max_messages: 2_000_000,
                ..Default::default()
            })
            .await
            .map_err(err)?;
        let consumer = stream
            .create_consumer(jetstream::consumer::pull::OrderedConfig {
                ..Default::default()
            })
            .await
            .map_err(err)?;
        consumer.messages().await.map_err(err)
    }

    pub async fn ensure_stream(
        context: &jetstream::Context,
        stream_name: &str,
        subject: &str,
    ) -> io::Result<()> {
        context
            .get_or_create_stream(jetstream::stream::Config {
                name: stream_name.to_string(),
                subjects: vec![subject.to_string()],
                max_messages: 2_000_000,
                ..Default::default()
            })
            .await
            .map_err(err)?;
        Ok(())
    }
}

pub fn echo_js(bind: &str, _msg_size: usize) -> io::Result<()> {
    let base = format!("wg{}", std::process::id());
    let rt = runtime()?;
    rt.block_on(async {
        let client = connect_client(bind).await?;
        let context = async_nats::jetstream::new(client.clone());
        let mut messages =
            js::stream_and_consumer(&context, &format!("{base}C2S"), &format!("{base}.c2s"))
                .await?;
        js::ensure_stream(&context, &format!("{base}S2C"), &format!("{base}.s2c")).await?;
        let out = format!("{base}.s2c");

        println!("READY {bind}/{base}");
        use std::io::Write;
        io::stdout().flush()?;

        while let Some(msg) = messages.next().await {
            let msg = msg.map_err(err)?;
            client
                .publish(out.clone(), msg.payload.clone())
                .await
                .map_err(err)?;
            client.flush().await.map_err(err)?;
        }
        Ok(())
    })
}

pub fn connect_js(
    addr: &str,
    _msg_size: usize,
) -> io::Result<(Box<dyn Sender>, Box<dyn Receiver>)> {
    let (hp, base) = split_addr(addr);
    let rt = runtime()?;
    let (client, messages) = rt.block_on(async {
        let client = connect_client(hp).await?;
        let context = async_nats::jetstream::new(client.clone());
        // The echo side created both streams before READY; get-or-create is
        // an idempotent open here.
        let messages =
            js::stream_and_consumer(&context, &format!("{base}S2C"), &format!("{base}.s2c"))
                .await?;
        Ok::<_, io::Error>((client, messages))
    })?;
    let tx = NatsSender {
        rt: Arc::clone(&rt),
        client,
        subject: format!("{base}.c2s"),
    };
    let rx = JsReceiver {
        rt,
        messages: Some(messages),
    };
    Ok((Box::new(tx), Box::new(rx)))
}

struct JsReceiver {
    rt: Arc<Runtime>,
    /// Option for the same Drop-needs-a-runtime reason as NatsReceiver.
    messages: Option<async_nats::jetstream::consumer::pull::Ordered>,
}

impl Drop for JsReceiver {
    fn drop(&mut self) {
        let _guard = self.rt.enter();
        drop(self.messages.take());
    }
}

impl Receiver for JsReceiver {
    fn recv(&mut self, buf: &mut [u8]) -> io::Result<RecvOutcome> {
        let messages = self.messages.as_mut().expect("present until drop");
        let msg = self
            .rt
            .block_on(async { tokio::time::timeout(RECV_TIMEOUT, messages.next()).await });
        match msg {
            Ok(Some(m)) => {
                let m = m.map_err(err)?;
                if m.payload.len() != buf.len() {
                    return Err(err(format!(
                        "message of {} bytes, expected {}",
                        m.payload.len(),
                        buf.len()
                    )));
                }
                buf.copy_from_slice(&m.payload);
                Ok(RecvOutcome::Msg)
            }
            Ok(None) => Ok(RecvOutcome::Closed),
            Err(_elapsed) => Ok(RecvOutcome::TimedOut),
        }
    }
}
