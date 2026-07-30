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

use rustls::pki_types::pem::PemObject;
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
    let certs: Vec<_> = CertificateDer::pem_file_iter(path)
        .map_err(|e| pem_error(path, &e))?
        .collect::<Result<_, _>>()
        .map_err(|e| pem_error(path, &e))?;
    if certs.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{}: no certificates in file", path.display()),
        ));
    }
    Ok(certs)
}

fn read_key(path: &Path) -> io::Result<PrivateKeyDer<'static>> {
    PrivateKeyDer::from_pem_file(path).map_err(|e| pem_error(path, &e))
}

fn pem_error(path: &Path, e: &dyn std::fmt::Display) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("{}: {e}", path.display()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A temp file that removes itself, so a failing test leaves nothing behind.
    struct Written(std::path::PathBuf);

    impl Written {
        fn new(name: &str, contents: &str) -> Self {
            let path = std::env::temp_dir().join(format!("bx-tls-{}-{name}", std::process::id()));
            std::fs::write(&path, contents).unwrap();
            Self(path)
        }
    }

    impl Drop for Written {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    fn message(cert: &Path, key: &Path) -> String {
        server_config(cert, key)
            .err()
            .map(|e| e.to_string())
            .unwrap_or_else(|| panic!("the identity was accepted"))
    }

    #[test]
    fn a_missing_certificate_names_the_file() {
        // What an operator sees when a path is wrong: a venue that stops at
        // startup saying which file, not one that starts and fails the first
        // handshake.
        let key = Written::new("absent.key", "");
        let absent = std::path::Path::new("no-such-certificate.pem");
        let said = message(absent, &key.0);
        assert!(said.contains("no-such-certificate.pem"), "{said}");
    }

    #[test]
    fn a_file_that_is_not_pem_is_refused() {
        let cert = Written::new("garbage.crt", "this is not a certificate\n");
        let key = Written::new("garbage.key", "nor is this a key\n");
        let said = message(&cert.0, &key.0);
        assert!(said.contains("garbage.crt"), "{said}");
    }

    #[test]
    fn a_certificate_without_its_key_is_refused() {
        // The failure that only shows at handshake time if nothing checks it
        // here: two files that are each valid PEM and do not belong together.
        let first = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let second = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let cert = Written::new("mismatch.crt", &first.cert.pem());
        let key = Written::new("mismatch.key", &second.key_pair.serialize_pem());
        let said = message(&cert.0, &key.0);
        assert!(
            said.contains("do not make a working identity"),
            "a certificate and a stranger's key were accepted: {said}"
        );
    }

    #[test]
    fn a_matching_pair_loads() {
        let made = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let cert = Written::new("good.crt", &made.cert.pem());
        let key = Written::new("good.key", &made.key_pair.serialize_pem());
        server_config(&cert.0, &key.0).expect("a matching pair is a working identity");
    }
}
