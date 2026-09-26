//! Assets and their precision.

use std::{fmt, sync::Arc};

use crate::error::DomainError;

/// Ticker of an asset such as `BTC`, `USD` or `USDC`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AssetCode(Arc<str>);

impl AssetCode {
    /// Accepts 2 to 10 uppercase letters or digits.
    pub fn parse(raw: &str) -> Result<Self, DomainError> {
        if is_asset_code(raw) {
            Ok(Self(Arc::from(raw)))
        } else {
            Err(DomainError::InvalidAssetCode)
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for AssetCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

pub(crate) fn is_asset_code(code: &str) -> bool {
    (2..=10).contains(&code.len())
        && code
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
}

/// Something the exchange can hold and trade, such as a coin or a currency.
///
/// `decimals` is the finest quantity the asset can be split into. A market
/// cannot trade the asset in smaller lots than that.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Asset {
    code: AssetCode,
    name: String,
    decimals: u32,
}

impl Asset {
    pub const MAX_DECIMALS: u32 = 18;
    pub const MAX_NAME_LEN: usize = 64;

    pub fn new(code: AssetCode, name: &str, decimals: u32) -> Result<Self, DomainError> {
        let name_ok = !name.trim().is_empty()
            && name.len() <= Self::MAX_NAME_LEN
            && name.bytes().all(|b| b.is_ascii_graphic() || b == b' ');
        if !name_ok {
            return Err(DomainError::InvalidAsset(
                "name must be 1 to 64 printable ASCII characters",
            ));
        }
        if decimals > Self::MAX_DECIMALS {
            return Err(DomainError::InvalidAsset("decimals must be 18 or fewer"));
        }
        Ok(Self {
            code,
            name: name.to_owned(),
            decimals,
        })
    }

    pub fn code(&self) -> &AssetCode {
        &self.code
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn decimals(&self) -> u32 {
        self.decimals
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn asset_codes_are_short_uppercase_tokens() {
        assert!(AssetCode::parse("USDC").is_ok());
        for bad in ["b", "btc", "BTC-USD", "TOOLONGCODE1", ""] {
            assert_eq!(
                AssetCode::parse(bad),
                Err(DomainError::InvalidAssetCode),
                "{bad}"
            );
        }
    }

    #[test]
    fn assets_validate_name_and_precision() {
        let code = AssetCode::parse("BTC").unwrap();
        assert!(Asset::new(code.clone(), "Bitcoin", 8).is_ok());
        assert!(Asset::new(code.clone(), " ", 8).is_err());
        assert!(Asset::new(code, "Bitcoin", 19).is_err());
    }
}
