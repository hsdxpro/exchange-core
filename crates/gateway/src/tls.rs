//! TLS 1.3 for sessions that arrive over the internet.
//!
//! Two kinds of client connect to a venue and they want opposite things. A
//! market maker on a colocated cross-connect wants nothing between it and the
//! book, and its wire is private by construction — that path stays raw. A
//! client on the internet has no private wire, and for it encryption is not
//! optional: the Ed25519 logon proves who a session is, but on a plaintext
//! stream an attacker who can write to the wire can still inject orders after
//! the logon. TLS is what extends the session's integrity from the handshake to
//! every byte after it.
//!
//! So the venue offers both, as two listeners: `listen` raw for the
//! cross-connect, `tls_listen` for everyone else. TLS 1.3 only — 1.2 removed a
//! round trip's worth of downgrade surface and every client young enough to use
//! this venue speaks 1.3.
//!
//! The private key is loaded from a file the operator holds, like the chain
//! key: a key inline in configuration is a key in every backup of it.

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::{ServerConfig, version::TLS13};
use std::io;
use std::path::Path;
use std::sync::Arc;

/// Builds the listener's TLS configuration from PEM files on disk.
///
/// # Errors
/// Fails if either file is unreadable, holds no usable PEM, or the certificate
/// and key do not match.
pub fn server_config(cert_file: &Path, key_file: &Path) -> io::Result<Arc<ServerConfig>> {
    let certs = read_certs(cert_file)?;
    let key = read_key(key_file)?;
    let config = ServerConfig::builder_with_protocol_versions(&[&TLS13])
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("certificate and key do not make a working identity: {e}"),
            )
        })?;
    Ok(Arc::new(config))
}

fn read_certs(path: &Path) -> io::Result<Vec<CertificateDer<'static>>> {
    let mut file = io::BufReader::new(std::fs::File::open(path).map_err(|e| at(path, &e))?);
    let certs: Vec<_> = rustls_pemfile::certs(&mut file).collect::<Result<_, _>>()?;
    if certs.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{}: no certificates in file", path.display()),
        ));
    }
    Ok(certs)
}

fn read_key(path: &Path) -> io::Result<PrivateKeyDer<'static>> {
    let mut file = io::BufReader::new(std::fs::File::open(path).map_err(|e| at(path, &e))?);
    rustls_pemfile::private_key(&mut file)?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{}: no usable private key in file", path.display()),
        )
    })
}

fn at(path: &Path, e: &io::Error) -> io::Error {
    io::Error::new(e.kind(), format!("{}: {e}", path.display()))
}
