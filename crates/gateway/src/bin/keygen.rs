//! Generates an account keypair.
//!
//! Exists so nobody has to invent one, and so nobody reuses the example in
//! `venue.conf`. A venue's security rests on the private half being private, and
//! the most common way that fails is a key copied out of somebody's
//! documentation.
//!
//! The private half is printed once, to standard output, and never written
//! anywhere. Keep it out of the repository: give it to the client that will sign
//! with it and put only the public half in the venue's configuration.

use bx_protocol::PUBLIC_KEY_LEN;
use ed25519_dalek::SigningKey;

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn main() {
    // Straight from the OS. A key from a counter, a clock or a passphrase is a
    // key somebody else can arrive at.
    let mut seed = [0_u8; PUBLIC_KEY_LEN];
    getrandom::fill(&mut seed).expect("the OS must be able to supply randomness");
    let signing = SigningKey::from_bytes(&seed);
    let public = signing.verifying_key().to_bytes();

    println!("# Give this to the client. It never goes in the venue's config,");
    println!("# and it never goes in a repository.");
    println!("private_key = {}", hex(&signing.to_bytes()));
    println!();
    println!("# Put this in the venue's config, under a [credential] block.");
    println!("[credential]");
    println!("account = <account id>");
    println!("public_key = {}", hex(&public));
}
