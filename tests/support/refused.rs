//! An origin that refuses every connection for as long as its guard lives.
//!
//! Binding a listener and dropping it frees the port, and a server started
//! concurrently by another test can take it and answer in its place (a 404
//! where a refused connection was expected). A socket that is bound but never
//! listens keeps the port: the kernel resets every connection to it, and no
//! other socket can bind it until the guard is dropped.

use std::net::SocketAddr;

use tokio::net::TcpSocket;

/// A refusing origin. Keep it alive for as long as the test sends to it.
pub struct Refused {
    _socket: TcpSocket,
    addr: SocketAddr,
}

impl Refused {
    /// Bind an ephemeral loopback port without listening on it.
    pub fn bind() -> Self {
        let socket = TcpSocket::new_v4().expect("a TCP socket");
        socket
            .bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .expect("an ephemeral loopback port");
        let addr = socket.local_addr().expect("the bound address");
        Self {
            _socket: socket,
            addr,
        }
    }

    /// The origin's base URL.
    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }
}
