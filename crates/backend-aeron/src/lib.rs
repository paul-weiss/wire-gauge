//! Aeron — the trading-industry reference, and round 1's deliberate hinge:
//! the same API measured as shared-memory IPC (`aeron-ipc`) and as UDP
//! unicast (`aeron-udp`), so the delta against the raw transports prices
//! Aeron's log-buffer + reliability machinery on each medium.
//!
//! This is the quarantined FFI backend: `rusteron` builds the vendored
//! Aeron C code and exposes it over generated wrappers. The media driver
//! runs *embedded in the echo child* (rusteron-media-driver), so driver
//! lifetime equals echo-child lifetime and the runner's kill sweeps it;
//! the measuring side attaches to the same driver directory announced in
//! READY. One Aeron client per thread — the wrappers hold raw pointers and
//! are treated as single-threaded, so the sender's client lives on the
//! caller thread and the receiver builds its own client lazily inside the
//! receive thread, the same pattern as the iceoryx2 backend.
//!
//! Configuration, for the report (the one documented tuning, and it is
//! Aeron's published low-latency profile in its single-core form):
//! driver `AERON_THREADING_MODE=SHARED` with
//! `AERON_SHARED_IDLE_STRATEGY=spin` — one spinning driver thread instead
//! of a backoff idle that parks between messages. Client conductors keep
//! their default idle. Receive is a busy poll with strided clock checks,
//! matching the shm and iceoryx2 backends. `offer` retries every error
//! until a 5s deadline: back-pressure and not-yet-connected both surface
//! as send lag rather than as invented message loss.

use std::cell::RefCell;
use std::ffi::CString;
use std::io;
use std::rc::Rc;
use std::time::{Duration, Instant};

use rusteron_client::{
    Aeron, AeronContext, AeronErrorHandlerLogger, AeronFragmentHandlerCallback, AeronHeader,
    AeronPublication, AeronSubscription, Handler, Handlers,
};

use harness::transport::{Receiver, RecvOutcome, Sender};

const STREAM_C2S: i32 = 1001;
const STREAM_S2C: i32 = 1002;
const UDP_PORT_C2S: u16 = 20121;
const UDP_PORT_S2C: u16 = 20122;

const SETUP_TIMEOUT: Duration = Duration::from_secs(10);
const OFFER_TIMEOUT: Duration = Duration::from_secs(5);
const RECV_TIMEOUT: Duration = Duration::from_secs(1);
const SPIN_POLLS: u32 = 1_000;
const CLOCK_STRIDE: u32 = 256;

/// Fits one Aeron fragment; keeps the fragment-assembler out of the path.
const MAX_MSG: usize = 1_024;

#[derive(Clone, Copy)]
pub enum Mode {
    Ipc,
    Udp,
}

impl Mode {
    fn channel(self, direction: &str) -> CString {
        let s = match (self, direction) {
            (Mode::Ipc, _) => "aeron:ipc".to_string(),
            (Mode::Udp, "c2s") => format!("aeron:udp?endpoint=127.0.0.1:{UDP_PORT_C2S}"),
            (Mode::Udp, _) => format!("aeron:udp?endpoint=127.0.0.1:{UDP_PORT_S2C}"),
        };
        CString::new(s).expect("no NUL in channel")
    }
}

pub fn default_bind(mode: Mode) -> String {
    let tag = match mode {
        Mode::Ipc => "aeron-ipc",
        Mode::Udp => "aeron-udp",
    };
    std::env::temp_dir()
        .join(format!("wire-gauge-{}-{tag}", std::process::id()))
        .to_string_lossy()
        .into_owned()
}

fn err(e: impl std::fmt::Debug) -> io::Error {
    io::Error::other(format!("aeron: {e:?}"))
}

fn cstr(s: &str) -> CString {
    CString::new(s).expect("no NUL in path")
}

/// Attach a client to the driver at `dir`, retrying while the driver spins
/// up (the echo child launches it moments before announcing READY).
fn attach_client(dir: &str) -> io::Result<(AeronContext, Aeron)> {
    let deadline = Instant::now() + SETUP_TIMEOUT;
    loop {
        let result = (|| -> Result<(AeronContext, Aeron), Box<dyn std::error::Error>> {
            let ctx = AeronContext::new()?;
            ctx.set_dir(&cstr(dir))?;
            let error_handler = Handler::new(AeronErrorHandlerLogger);
            ctx.set_error_handler(Some(error_handler.clone()))?;
            std::mem::forget(error_handler); // lives as long as the process
            let aeron = Aeron::new(&ctx)?;
            aeron.start()?;
            Ok((ctx, aeron))
        })();
        match result {
            Ok(pair) => return Ok(pair),
            Err(e) => {
                if Instant::now() > deadline {
                    return Err(err(format!("client could not attach to {dir}: {e}")));
                }
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    }
}

fn add_publication(aeron: &Aeron, channel: &CString, stream: i32) -> io::Result<AeronPublication> {
    aeron
        .async_add_publication(channel, stream)
        .map_err(err)?
        .poll_blocking(SETUP_TIMEOUT)
        .map_err(err)
}

fn add_subscription(
    aeron: &Aeron,
    channel: &CString,
    stream: i32,
) -> io::Result<AeronSubscription> {
    aeron
        .async_add_subscription(channel, stream, Handlers::NONE, Handlers::NONE)
        .map_err(err)?
        .poll_blocking(SETUP_TIMEOUT)
        .map_err(err)
}

/// Offer one message, retrying every failure (back-pressure, not yet
/// connected) until the deadline. Time spent here is send lag — the honest
/// place for it.
fn offer_blocking(publication: &AeronPublication, msg: &[u8]) -> io::Result<()> {
    let deadline = Instant::now() + OFFER_TIMEOUT;
    loop {
        match publication.offer(msg) {
            Ok(_) => return Ok(()),
            Err(e) => {
                if Instant::now() > deadline {
                    return Err(err(format!("offer failed past deadline: {e}")));
                }
                std::hint::spin_loop();
            }
        }
    }
}

// ------------------------------------------------------------------- echo

pub fn echo(mode: Mode, bind: &str, msg_size: usize) -> io::Result<()> {
    use rusteron_media_driver::{AeronDriver, AeronDriverContext};

    assert!(msg_size <= MAX_MSG, "message must fit one fragment");

    // The C driver reads these from the environment at context creation.
    std::env::set_var("AERON_THREADING_MODE", "SHARED");
    std::env::set_var("AERON_SHARED_IDLE_STRATEGY", "spin");
    std::fs::create_dir_all(bind)?;
    let driver_ctx = AeronDriverContext::new().map_err(err)?;
    driver_ctx.set_dir(&cstr(bind)).map_err(err)?;
    driver_ctx.set_dir_delete_on_start(true).map_err(err)?;
    let (_stop, _driver) = AeronDriver::launch_embedded(driver_ctx.clone(), false);

    let (_ctx, aeron) = attach_client(bind)?;
    let subscription = add_subscription(&aeron, &mode.channel("c2s"), STREAM_C2S)?;
    let publication = add_publication(&aeron, &mode.channel("s2c"), STREAM_S2C)?;

    println!("READY {bind}");
    use std::io::Write;
    io::stdout().flush()?;

    struct EchoHandler {
        publication: AeronPublication,
    }
    impl AeronFragmentHandlerCallback for EchoHandler {
        fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], _header: AeronHeader) {
            // Killed by the runner at end of run; a genuinely dead
            // publication would spin out the offer deadline and land here.
            offer_blocking(&self.publication, buffer).expect("echo offer");
        }
    }
    let handler = Handler::new(EchoHandler { publication });
    loop {
        let polled = subscription.poll(Some(&handler), 1).map_err(err)?;
        if polled == 0 {
            std::hint::spin_loop();
        }
    }
}

// ---------------------------------------------------------------- connect

pub fn connect(
    mode: Mode,
    addr: &str,
    msg_size: usize,
) -> io::Result<(Box<dyn Sender>, Box<dyn Receiver>)> {
    assert!(msg_size <= MAX_MSG, "message must fit one fragment");
    let (ctx, aeron) = attach_client(addr)?;
    let publication = add_publication(&aeron, &mode.channel("c2s"), STREAM_C2S)?;

    // The echo side's subscription exists before READY, so this connects
    // fast; offering before it does would silently drop.
    let deadline = Instant::now() + SETUP_TIMEOUT;
    while !publication.is_connected() {
        if Instant::now() > deadline {
            return Err(err("publication never connected to echo subscription"));
        }
        std::thread::sleep(Duration::from_millis(1));
    }

    let tx = AeronSender {
        _ctx: ctx,
        _client: aeron,
        publication,
    };
    let rx = LazyReceiver {
        dir: addr.to_string(),
        channel: mode.channel("s2c"),
        msg_size,
        inner: None,
    };
    Ok((Box::new(tx), Box::new(rx)))
}

struct AeronSender {
    _ctx: AeronContext,
    _client: Aeron,
    publication: AeronPublication,
}

impl Sender for AeronSender {
    fn send(&mut self, msg: &[u8]) -> io::Result<()> {
        offer_blocking(&self.publication, msg)
    }
}

/// State shared between the poll callback and `recv`, same-thread only.
struct RxState {
    len: Option<usize>,
    buf: Vec<u8>,
}

struct FragmentToBuf(Rc<RefCell<RxState>>);

impl AeronFragmentHandlerCallback for FragmentToBuf {
    fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], _header: AeronHeader) {
        let mut state = self.0.borrow_mut();
        state.buf.clear();
        state.buf.extend_from_slice(buffer);
        state.len = Some(buffer.len());
    }
}

struct ConnectedReceiver {
    _ctx: AeronContext,
    _client: Aeron,
    subscription: AeronSubscription,
    handler: Handler<FragmentToBuf>,
    state: Rc<RefCell<RxState>>,
}

/// Owns nothing until the first `recv` constructs the Aeron client on the
/// receive thread — the rusteron wrappers hold raw C pointers and are
/// treated as single-threaded.
pub struct LazyReceiver {
    dir: String,
    channel: CString,
    msg_size: usize,
    inner: Option<ConnectedReceiver>,
}

// SAFETY: `inner` is None whenever this value crosses threads — `connect`
// returns it unconstructed, the harness moves it into the receive thread
// exactly once, and the first `recv` there builds the client, which then
// never leaves that thread.
unsafe impl Send for LazyReceiver {}

impl Receiver for LazyReceiver {
    fn recv(&mut self, buf: &mut [u8]) -> io::Result<RecvOutcome> {
        if self.inner.is_none() {
            let (ctx, aeron) = attach_client(&self.dir)?;
            let subscription = add_subscription(&aeron, &self.channel, STREAM_S2C)?;
            let state = Rc::new(RefCell::new(RxState {
                len: None,
                buf: Vec::with_capacity(self.msg_size),
            }));
            let handler = Handler::new(FragmentToBuf(Rc::clone(&state)));
            self.inner = Some(ConnectedReceiver {
                _ctx: ctx,
                _client: aeron,
                subscription,
                handler,
                state,
            });
        }
        let inner = self.inner.as_ref().expect("just constructed");

        let mut misses: u32 = 0;
        let mut deadline: Option<Instant> = None;
        loop {
            let polled = inner
                .subscription
                .poll(Some(&inner.handler), 1)
                .map_err(err)?;
            if polled > 0 {
                let mut state = inner.state.borrow_mut();
                let len = state
                    .len
                    .take()
                    .ok_or_else(|| err("poll>0 but no fragment"))?;
                if len != buf.len() {
                    return Err(err(format!(
                        "fragment of {len} bytes, expected {}",
                        buf.len()
                    )));
                }
                buf.copy_from_slice(&state.buf);
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
