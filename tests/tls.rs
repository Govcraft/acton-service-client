//! TLS material configured on the builder: a private CA (the trust-only path),
//! a client certificate for mutual TLS, and the builder's timeout applying to
//! the TLS-configured client it constructs.
//!
//! The server side is a minimal rustls listener over certificates minted per
//! test with `rcgen`, so every test owns its CA and nothing depends on the
//! machine's trust store.

use std::sync::Arc;
use std::time::{Duration, Instant};

use acton_service_client::{ClientError, Method, ServiceClient};
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa, KeyPair,
};
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::server::WebPkiClientVerifier;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

/// The timeout under test: short, so a stall costs half a second.
const TIMEOUT: Duration = Duration::from_millis(500);

/// How long a stalled call may take before the test calls it unbounded. It is
/// shorter than the builder's default timeout (30s), so a client that ignored
/// `.timeout()` fails here instead of passing late.
const BOUND: Duration = Duration::from_secs(10);

/// A private CA, and a leaf it issued, both as PEM plus DER for the server.
struct Pki {
    ca: CertifiedIssuer<'static, KeyPair>,
}

impl Pki {
    fn new() -> Self {
        let mut params = CertificateParams::new(Vec::<String>::new()).expect("CA params");
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let key = KeyPair::generate().expect("a CA key");
        let ca = CertifiedIssuer::self_signed(params, key).expect("a self-signed CA");
        Self { ca }
    }

    fn ca_pem(&self) -> String {
        self.ca.pem()
    }

    fn ca_der(&self) -> CertificateDer<'static> {
        self.ca.der().clone()
    }

    /// A leaf for `127.0.0.1` with the given usage, as (cert PEM, key PEM,
    /// cert DER, key DER).
    fn leaf(
        &self,
        usage: ExtendedKeyUsagePurpose,
    ) -> (
        String,
        String,
        CertificateDer<'static>,
        PrivateKeyDer<'static>,
    ) {
        let mut params =
            CertificateParams::new(vec!["127.0.0.1".to_string()]).expect("leaf params");
        params.extended_key_usages = vec![usage];
        let key = KeyPair::generate().expect("a leaf key");
        let cert = params.signed_by(&key, &self.ca).expect("a signed leaf");
        let key_der = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.serialize_der()));
        (cert.pem(), key.serialize_pem(), cert.der().clone(), key_der)
    }
}

/// What the server does once the handshake completes.
#[derive(Clone, Copy)]
enum After {
    /// Reads the request and answers `200 {"ok":true}`.
    Answer,
    /// Reads the request and never writes a byte.
    Stall,
}

/// Starts a TLS server on loopback and returns its base URL.
///
/// With `client_roots`, the server requires a client certificate issued by
/// one of them.
async fn serve(pki: &Pki, client_roots: Option<CertificateDer<'static>>, after: After) -> String {
    let (_, _, cert, key) = pki.leaf(ExtendedKeyUsagePurpose::ServerAuth);
    let builder = rustls::ServerConfig::builder();
    let config = match client_roots {
        Some(root) => {
            let mut roots = RootCertStore::empty();
            roots.add(root).expect("the client CA");
            let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
                .build()
                .expect("a client verifier");
            builder.with_client_cert_verifier(verifier)
        }
        None => builder.with_no_client_auth(),
    }
    .with_single_cert(vec![cert], key)
    .expect("a server config");
    let acceptor = TlsAcceptor::from(Arc::new(config));

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("a port");
    let url = format!("https://{}", listener.local_addr().expect("bound"));
    tokio::spawn(async move {
        while let Ok((tcp, _)) = listener.accept().await {
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                let Ok(mut tls) = acceptor.accept(tcp).await else {
                    return;
                };
                let mut buf = vec![0_u8; 8192];
                let mut read = Vec::new();
                while !read.windows(4).any(|w| w == b"\r\n\r\n") {
                    match tls.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => read.extend_from_slice(&buf[..n]),
                    }
                }
                match after {
                    After::Answer => {
                        let body = r#"{"ok":true}"#;
                        let response = format!(
                            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\
                             content-length: {}\r\nconnection: close\r\n\r\n{body}",
                            body.len()
                        );
                        let _ = tls.write_all(response.as_bytes()).await;
                        let _ = tls.shutdown().await;
                    }
                    After::Stall => {
                        tokio::time::sleep(Duration::from_secs(3600)).await;
                        drop(tls);
                    }
                }
            });
        }
    });
    url
}

#[derive(serde::Deserialize)]
struct Probe {
    ok: bool,
}

async fn get_ok(client: &ServiceClient) -> Result<Probe, ClientError> {
    client
        .request_unversioned(Method::GET, "probe")
        .send_json::<Probe>()
        .await
}

#[tokio::test]
async fn a_private_ca_is_trusted_with_root_certificate_pem() {
    let pki = Pki::new();
    let url = serve(&pki, None, After::Answer).await;

    let trusted = ServiceClient::builder(&url)
        .root_certificate_pem(pki.ca_pem())
        .build()
        .expect("a usable CA bundle");
    assert!(
        get_ok(&trusted)
            .await
            .expect("the private CA is trusted")
            .ok
    );

    let untrusted = ServiceClient::builder(&url).build().expect("a client");
    let Err(error) = get_ok(&untrusted).await else {
        panic!("without the CA the server's certificate is unknown");
    };
    assert!(matches!(error, ClientError::Transport(_)), "{error:?}");
}

#[tokio::test]
async fn the_builder_timeout_bounds_a_tls_configured_client() {
    let pki = Pki::new();
    let url = serve(&pki, None, After::Stall).await;
    let client = ServiceClient::builder(&url)
        .root_certificate_pem(pki.ca_pem())
        .timeout(TIMEOUT)
        .build()
        .expect("a usable CA bundle");

    let started = Instant::now();
    let outcome = tokio::time::timeout(BOUND, get_ok(&client))
        .await
        .expect("the call returns within the bound, so the timeout applies");
    let elapsed = started.elapsed();

    let Err(error) = outcome else {
        panic!("a server that never answers is an error");
    };
    let ClientError::Transport(source) = &error else {
        panic!("a stall is a transport failure: {error:?}");
    };
    assert!(source.is_timeout(), "the attempt timed out: {source}");
    assert!(
        elapsed >= TIMEOUT,
        "the call ended before its timeout ({elapsed:?}), so something else ended it"
    );
}

#[tokio::test]
async fn identity_pem_presents_a_client_certificate() {
    let pki = Pki::new();
    let url = serve(&pki, Some(pki.ca_der()), After::Answer).await;
    let (cert_pem, key_pem, _, _) = pki.leaf(ExtendedKeyUsagePurpose::ClientAuth);

    let with_identity = ServiceClient::builder(&url)
        .root_certificate_pem(pki.ca_pem())
        .identity_pem(cert_pem, key_pem)
        .build()
        .expect("a usable identity");
    assert!(
        get_ok(&with_identity)
            .await
            .expect("the server accepts the client certificate")
            .ok
    );

    let without = ServiceClient::builder(&url)
        .root_certificate_pem(pki.ca_pem())
        .build()
        .expect("a client");
    let Err(error) = get_ok(&without).await else {
        panic!("a server that requires a client certificate refuses one without");
    };
    assert!(matches!(error, ClientError::Transport(_)), "{error:?}");
}

#[test]
fn unusable_tls_material_is_a_config_error_that_never_echoes_the_key() {
    const SECRET: &str = "c2VjcmV0LWtleS1ieXRlcy00NDE3";
    let key = format!("-----BEGIN PRIVATE KEY-----\n{SECRET}\n-----END PRIVATE KEY-----\n");

    let Err(error) = ServiceClient::builder("https://127.0.0.1:1")
        .identity_pem("not a certificate", key)
        .build()
    else {
        panic!("an unusable identity is refused");
    };
    let ClientError::Config(message) = &error else {
        panic!("a config error: {error:?}");
    };
    assert!(
        !message.contains(SECRET),
        "the key reached the error: {message}"
    );

    let Err(error) = ServiceClient::builder("https://127.0.0.1:1")
        .root_certificate_pem("not a certificate")
        .build()
    else {
        panic!("a bundle with no certificate is refused");
    };
    assert!(matches!(error, ClientError::Config(_)), "{error:?}");
}

#[test]
fn tls_material_alongside_a_supplied_client_is_refused() {
    let pki = Pki::new();

    let Err(error) = ServiceClient::builder("https://127.0.0.1:1")
        .with_http_client(reqwest_client())
        .root_certificate_pem(pki.ca_pem())
        .build()
    else {
        panic!("TLS material cannot reach a supplied client");
    };

    assert!(matches!(error, ClientError::Config(_)), "{error:?}");
}

/// A supplied client, built the way a caller would.
fn reqwest_client() -> acton_service_client::reqwest::Client {
    acton_service_client::reqwest::Client::builder()
        .build()
        .expect("a plain client")
}
