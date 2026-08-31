//! iceoryx2 — the "don't hand-roll it" shared-memory candidate: zero-copy
//! publish/subscribe over a decentralized shared-memory runtime.
//!
//! Configuration choices, for the report:
//! - The plain `ipc::Service` flavor, not `ipc_threadsafe` — the threadsafe
//!   flavor buys `Sync` with internal locking, and the harness never shares
//!   a port between threads. It does *move* the receiver into the receive
//!   thread, which single-threaded (Rc-based) ports forbid, so the receiver
//!   is constructed lazily inside that thread instead (see
//!   [`LazyReceiver`]).
//! - Dynamic `[u8]` payloads with `initial_max_slice_len(msg_size)`; every
//!   loan is exactly `msg_size`, so the data segment never reallocates.
//! - Subscriber buffer of 4096 samples with safe overflow (iceoryx2's
//!   default ring semantics): a stalled consumer loses oldest-first, and
//!   the harness's sequence accounting reports that as drops.
//! - Receive is a busy poll, matching the custom shm ring: latency-optimal
//!   and the configuration a trading hot path would run.
//!
//! One service per direction, named `<base>/c2s` and `<base>/s2c`. The
//! echo child creates both (open_or_create) before announcing READY. The
//! child is killed without ceremony at the end of a run, so both ends call
//! `try_cleanup_dead_nodes` at startup to reap any earlier run's corpses.

use std::io;
use std::time::{Duration, Instant};

use iceoryx2::node::Node;
use iceoryx2::port::publisher::Publisher;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;

use harness::transport::{Receiver, RecvOutcome, Sender};

type Factory =
    iceoryx2::service::port_factory::publish_subscribe::PortFactory<ipc::Service, [u8], ()>;
type IoxPublisher = Publisher<ipc::Service, [u8], ()>;
type IoxSubscriber = Subscriber<ipc::Service, [u8], ()>;

/// Samples the subscriber can hold before safe overflow starts dropping
/// oldest-first. Deep enough that only a genuinely stalled consumer hits it.
const BUFFER_SIZE: usize = 4096;

const RECV_TIMEOUT: Duration = Duration::from_secs(1);
/// Pure spin iterations before the receive loop starts consulting the
/// clock, and the clock-check stride afterwards — keeps `Instant::now()`
/// off the hot path where the next message is nanoseconds away.
const SPIN_POLLS: u32 = 1_000;
const CLOCK_STRIDE: u32 = 256;

pub fn default_bind() -> String {
    format!("wire-gauge/{}", std::process::id())
}

fn err(e: impl core::fmt::Debug) -> io::Error {
    io::Error::other(format!("{e:?}"))
}

fn reap_dead_nodes() {
    let _ = Node::<ipc::Service>::try_cleanup_dead_nodes(Config::global_config());
}

fn open_service(node: &Node<ipc::Service>, name: &str) -> io::Result<Factory> {
    node.service_builder(&name.try_into().map_err(err)?)
        .publish_subscribe::<[u8]>()
        .subscriber_max_buffer_size(BUFFER_SIZE)
        .max_publishers(1)
        .max_subscribers(1)
        .open_or_create()
        .map_err(err)
}

fn make_publisher(
    node: &Node<ipc::Service>,
    name: &str,
    msg_size: usize,
) -> io::Result<(Factory, IoxPublisher)> {
    let factory = open_service(node, name)?;
    let publisher = factory
        .publisher_builder()
        .initial_max_slice_len(msg_size)
        .create()
        .map_err(err)?;
    Ok((factory, publisher))
}

fn make_subscriber(node: &Node<ipc::Service>, name: &str) -> io::Result<(Factory, IoxSubscriber)> {
    let factory = open_service(node, name)?;
    let subscriber = factory
        .subscriber_builder()
        .buffer_size(BUFFER_SIZE)
        .create()
        .map_err(err)?;
    Ok((factory, subscriber))
}

pub fn echo(base: &str, msg_size: usize) -> io::Result<()> {
    reap_dead_nodes();
    let node = NodeBuilder::new().create::<ipc::Service>().map_err(err)?;
    let (_rx_factory, subscriber) = make_subscriber(&node, &format!("{base}/c2s"))?;
    let (_tx_factory, publisher) = make_publisher(&node, &format!("{base}/s2c"), msg_size)?;

    println!("READY {base}");
    use std::io::Write;
    io::stdout().flush()?;

    loop {
        match subscriber.receive().map_err(err)? {
            Some(sample) => {
                let out = publisher.loan_slice_uninit(sample.len()).map_err(err)?;
                out.write_from_slice(&sample).send().map_err(err)?;
            }
            None => std::hint::spin_loop(),
        }
    }
}

pub fn connect(base: &str, msg_size: usize) -> io::Result<(Box<dyn Sender>, Box<dyn Receiver>)> {
    reap_dead_nodes();
    let node = NodeBuilder::new().create::<ipc::Service>().map_err(err)?;
    let (factory, publisher) = make_publisher(&node, &format!("{base}/c2s"), msg_size)?;
    let tx = IoxSender {
        _node: node,
        _factory: factory,
        publisher,
    };
    let rx = LazyReceiver {
        service: format!("{base}/s2c"),
        inner: None,
    };
    Ok((Box::new(tx), Box::new(rx)))
}

struct IoxSender {
    _node: Node<ipc::Service>,
    _factory: Factory,
    publisher: IoxPublisher,
}

impl Sender for IoxSender {
    fn send(&mut self, msg: &[u8]) -> io::Result<()> {
        let sample = self.publisher.loan_slice_uninit(msg.len()).map_err(err)?;
        sample.write_from_slice(msg).send().map_err(err)?;
        Ok(())
    }
}

struct ConnectedReceiver {
    _node: Node<ipc::Service>,
    _factory: Factory,
    subscriber: IoxSubscriber,
}

/// A receiver that owns nothing until its first `recv`, which constructs
/// the node and subscriber on the calling thread.
pub struct LazyReceiver {
    service: String,
    inner: Option<ConnectedReceiver>,
}

// SAFETY: iceoryx2's single-threaded ports are Rc-based and must live and
// die on one thread. `inner` is None whenever this value crosses threads —
// `connect` returns it unconstructed, the harness moves it into the receive
// thread exactly once, and the first `recv` there builds the port, which
// then never leaves that thread.
unsafe impl Send for LazyReceiver {}

impl Receiver for LazyReceiver {
    fn recv(&mut self, buf: &mut [u8]) -> io::Result<RecvOutcome> {
        if self.inner.is_none() {
            reap_dead_nodes();
            let node = NodeBuilder::new().create::<ipc::Service>().map_err(err)?;
            let (factory, subscriber) = make_subscriber(&node, &self.service)?;
            self.inner = Some(ConnectedReceiver {
                _node: node,
                _factory: factory,
                subscriber,
            });
        }
        let subscriber = &self.inner.as_ref().expect("just constructed").subscriber;

        let mut misses: u32 = 0;
        let mut deadline: Option<Instant> = None;
        loop {
            if let Some(sample) = subscriber.receive().map_err(err)? {
                if sample.len() != buf.len() {
                    return Err(io::Error::other(format!(
                        "sample of {} bytes, expected {}",
                        sample.len(),
                        buf.len()
                    )));
                }
                buf.copy_from_slice(&sample);
                return Ok(RecvOutcome::Msg);
            }
            misses += 1;
            if misses >= SPIN_POLLS && misses.is_multiple_of(CLOCK_STRIDE) {
                match deadline {
                    None => deadline = Some(Instant::now() + RECV_TIMEOUT),
                    Some(d) => {
                        if Instant::now() > d {
                            return Ok(RecvOutcome::TimedOut);
                        }
                    }
                }
            }
            std::hint::spin_loop();
        }
    }
}
