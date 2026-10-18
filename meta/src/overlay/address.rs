use ed25519_dalek::{SigningKey, VerifyingKey};
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OverlayAddress(pub [u8; 32]);

impl OverlayAddress {
    pub fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn from_verifying_key(key: &VerifyingKey) -> Self {
        Self(key.to_bytes())
    }

    pub fn to_verifying_key(&self) -> Result<VerifyingKey, ed25519_dalek::SignatureError> {
        VerifyingKey::from_bytes(&self.0)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn to_hex(&self) -> String {
        let mut s = String::with_capacity(64);
        for byte in &self.0 {
            use std::fmt::Write;
            let _ = write!(s, "{:02x}", byte);
        }
        s
    }
}

impl fmt::Debug for OverlayAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "OverlayAddress({})", &self.to_hex()[..8])
    }
}

impl fmt::Display for OverlayAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

#[derive(Clone)]
pub struct Keypair {
    pub signing_key: SigningKey,
    pub address: OverlayAddress,
}

impl Keypair {
    pub fn generate() -> Self {
        let mut csprng = OsRng;
        let signing_key = SigningKey::generate(&mut csprng);
        let address = OverlayAddress::from_verifying_key(&signing_key.verifying_key());
        Self {
            signing_key,
            address,
        }
    }

    pub fn from_signing_key(signing_key: SigningKey) -> Self {
        let address = OverlayAddress::from_verifying_key(&signing_key.verifying_key());
        Self {
            signing_key,
            address,
        }
    }
}
