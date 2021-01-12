use std::fmt;
use std::future::Future;
// use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use hyper::server::accept::Accept;

use log::{debug, error};

use tokio::time::Delay;
// use tokio::io::{AsyncRead, AsyncWrite};
// use tokio::net::{TcpListener, TcpStream};
use std::net::{Shutdown, TcpListener, TcpStream};
use smol::{future, prelude::*, Async};

// TODO.async: 'Listener' and 'Connection' provide common enough functionality
// that they could be introduced in upstream libraries.
/// A 'Listener' yields incoming connections
pub trait Listener {
    type Connection: Connection;

    /// Return the actual address this listener bound to.
    fn local_addr(&self) -> Option<SocketAddr>;

    /// Try to accept an incoming Connection if ready
    fn poll_accept(&mut self, cx: &mut Context<'_>) -> Poll<std::io::Result<Self::Connection>>;
}

/// A 'Connection' represents an open connection to a client
pub trait Connection: tokio::io::AsyncRead + tokio::io::AsyncWrite {
    fn remote_addr(&self) -> Option<SocketAddr>;
}

/// This is a generic version of hyper's AddrIncoming that is intended to be
/// usable with listeners other than a plain TCP stream, e.g. TLS and/or Unix
/// sockets. It does so by bridging the `Listener` trait to what hyper wants (an
/// Accept). This type is internal to Rocket.
#[must_use = "streams do nothing unless polled"]
pub struct Incoming<L> {
    listener: L,
    sleep_on_errors: Option<Duration>,
    pending_error_delay: Option<Delay>,
}

impl<L: Listener> Incoming<L> {
    /// Construct an `Incoming` from an existing `Listener`.
    pub fn from_listener(listener: L) -> Self {
        Self {
            listener,
            sleep_on_errors: Some(Duration::from_secs(1)),
            pending_error_delay: None,
        }
    }

    /// Set whether to sleep on accept errors.
    ///
    /// A possible scenario is that the process has hit the max open files
    /// allowed, and so trying to accept a new connection will fail with
    /// `EMFILE`. In some cases, it's preferable to just wait for some time, if
    /// the application will likely close some files (or connections), and try
    /// to accept the connection again. If this option is `true`, the error
    /// will be logged at the `error` level, since it is still a big deal,
    /// and then the listener will sleep for 1 second.
    ///
    /// In other cases, hitting the max open files should be treat similarly
    /// to being out-of-memory, and simply error (and shutdown). Setting
    /// this option to `None` will allow that.
    ///
    /// Default is 1 second.
    pub fn set_sleep_on_errors(&mut self, val: Option<Duration>) {
        self.sleep_on_errors = val;
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<std::io::Result<L::Connection>> {
        // Check if a previous delay is active that was set by IO errors.
        if let Some(ref mut delay) = self.pending_error_delay {
            match Pin::new(delay).poll(cx) {
                Poll::Ready(()) => {}
                Poll::Pending => return Poll::Pending,
            }
        }
        self.pending_error_delay = None;

        loop {
            match self.listener.poll_accept(cx) {
                Poll::Ready(Ok(stream)) => {
                    return Poll::Ready(Ok(stream));
                },
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => {
                    // Connection errors can be ignored directly, continue by
                    // accepting the next request.
                    if is_connection_error(&e) {
                        debug!("accepted connection already errored: {}", e);
                        continue;
                    }

                    if let Some(duration) = self.sleep_on_errors {
                        error!("accept error: {}", e);

                        // Sleep for the specified duration
                        let mut error_delay = tokio::time::delay_for(duration);

                        match Pin::new(&mut error_delay).poll(cx) {
                            Poll::Ready(()) => {
                                // Wow, it's been a second already? Ok then...
                                continue
                            },
                            Poll::Pending => {
                                self.pending_error_delay = Some(error_delay);
                                return Poll::Pending;
                            },
                        }
                    } else {
                        return Poll::Ready(Err(e));
                    }
                },
            }
        }
    }
}

impl<L: Listener + Unpin> Accept for Incoming<L> {
    type Conn = L::Connection;
    type Error = std::io::Error;

    fn poll_accept(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>
    ) -> Poll<Option<std::io::Result<Self::Conn>>> {
        self.poll_next(cx).map(Some)
    }
}

/// This function defines errors that are per-connection. Which basically
/// means that if we get this error from `accept()` system call it means
/// next connection might be ready to be accepted.
///
/// All other errors will incur a delay before next `accept()` is performed.
/// The delay is useful to handle resource exhaustion errors like ENFILE
/// and EMFILE. Otherwise, could enter into tight loop.
fn is_connection_error(e: &std::io::Error) -> bool {
    match e.kind() {
        std::io::ErrorKind::ConnectionRefused |
        std::io::ErrorKind::ConnectionAborted |
        std::io::ErrorKind::ConnectionReset => true,
        _ => false,
    }
}

impl<L: fmt::Debug> fmt::Debug for Incoming<L> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Incoming")
            .field("listener", &self.listener)
            .finish()
    }
}

pub async fn bind_tcp(address: SocketAddr) -> std::io::Result<SmolListener> {
    Ok(SmolListener { inner: Async::<TcpListener>::bind(address)? })
}

impl Listener for SmolListener {
    type Connection = SmolStream;

    fn local_addr(&self) -> Option<SocketAddr> {
        self.inner.get_ref().local_addr().ok()
    }

    fn poll_accept(&mut self, cx: &mut Context<'_>) -> Poll<std::io::Result<Self::Connection>> {

        let incoming = self.inner.incoming();
        smol::pin!(incoming);
        let stream = smol::ready!(incoming.poll_next(cx)).unwrap()?;

        Poll::Ready(Ok(SmolStream{ inner: stream }))
        // self.inner.poll_accept(cx).map_ok(|(stream, _addr)| stream)
    }
}

pub struct SmolListener {
    inner: Async<TcpListener>,
}

pub struct SmolStream {
    inner: Async<TcpStream>,
}

impl Connection for SmolStream {
    fn remote_addr(&self) -> Option<SocketAddr> {
        self.inner.get_ref().peer_addr().ok()
    }
}


impl tokio::io::AsyncRead for SmolStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl tokio::io::AsyncWrite for SmolStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.inner.get_ref().shutdown(Shutdown::Write)?;
        Poll::Ready(Ok(()))
    }
}
