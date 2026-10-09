//! Concentrated liquidity pools store `sqrt(price)` as a fixed point
//! integer. This module turns it into a decimal price with integer math
//! only.

use alloy_primitives::{U256, U512};
use rust_decimal::Decimal;

/// Fractional digits computed before the result is trimmed to fit.
const SCALE: u32 = 18;
/// `rust_decimal` keeps a 96 bit mantissa.
const MAX_MANTISSA: u128 = (1 << 96) - 1;

/// Price of the base token in the quote token from a square root price.
///
/// `sqrt_price` is `sqrt(token1 / token0)` in raw units with
/// `fraction_bits` fractional bits: 96 for Uniswap v3 (`sqrtPriceX96`), 64
/// for Orca Whirlpools (`sqrt_price`, Q64.64). `base_is_token0` selects the
/// direction, and the decimals convert raw units to whole tokens.
///
/// The computation is exact up to 18 fractional digits, then truncated:
/// `num / den` is evaluated on 512 bit integers, where a 160 bit square
/// root squared, times 10^36, still fits. Returns `None` for a zero price
/// or a result too large for a `Decimal`.
pub fn price_from_sqrt(
    sqrt_price: U256,
    fraction_bits: usize,
    base_is_token0: bool,
    decimals0: u8,
    decimals1: u8,
) -> Option<Decimal> {
    if sqrt_price.is_zero() || fraction_bits > 128 {
        return None;
    }
    let squared = U512::from(sqrt_price).checked_mul(U512::from(sqrt_price))?;
    let one = U512::from(1_u8) << (2 * fraction_bits);
    let ten = U512::from(10_u8);
    let pow10 = |exponent: u32| ten.checked_pow(U512::from(exponent));
    let shift = u32::from(decimals0.abs_diff(decimals1));
    let (token0_has_more, scale) = (decimals0 >= decimals1, pow10(SCALE)?);

    // token1 per token0 in whole tokens = squared / one * 10^(d0 - d1).
    let (mut numerator, mut denominator) = if base_is_token0 {
        (squared, one)
    } else {
        (one, squared)
    };
    numerator = numerator.checked_mul(scale)?;
    if token0_has_more == base_is_token0 {
        numerator = numerator.checked_mul(pow10(shift)?)?;
    } else {
        denominator = denominator.checked_mul(pow10(shift)?)?;
    }

    let mut quotient = numerator.checked_div(denominator)?;
    let mut scale = SCALE;
    let limit = U512::from(MAX_MANTISSA);
    while quotient > limit {
        if scale == 0 {
            return None;
        }
        quotient /= ten;
        scale -= 1;
    }
    let mantissa = i128::try_from(u128::try_from(quotient).ok()?).ok()?;
    let price = Decimal::try_from_i128_with_scale(mantissa, scale).ok()?;
    (!price.is_zero()).then(|| price.normalize())
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use rust_decimal_macros::dec;

    use super::*;

    fn u(text: &str) -> U256 {
        U256::from_str(text).unwrap()
    }

    // Expected values were computed independently with Python's arbitrary
    // precision integers from the recorded mainnet data.

    #[test]
    fn uniswap_usdc_weth_prices_eth_in_usdc() {
        // USDC (6 decimals) is token0, WETH (18) is token1, base is WETH.
        let sqrt = u("1591477429476286610218534748006820");
        assert_eq!(
            price_from_sqrt(sqrt, 96, false, 6, 18),
            Some(dec!(2478.324669500408464656))
        );
    }

    #[test]
    fn uniswap_wbtc_usdc_prices_btc_in_usdc() {
        // WBTC (8 decimals) is token0 and the base, USDC (6) is token1.
        let slot0 = include_str!("../../tests/fixtures/eth_call_slot0.json");
        let value: serde_json::Value = serde_json::from_str(slot0).unwrap();
        let word = &value["0x99ac8ca7087fa4a2a1fb6357269965a2014abc35"]["result"]
            .as_str()
            .unwrap()[2..66];
        let sqrt = U256::from_str_radix(word, 16).unwrap();
        assert_eq!(
            price_from_sqrt(sqrt, 96, true, 8, 6),
            Some(dec!(81500.064982892932354632))
        );
    }

    #[test]
    fn whirlpool_q64_prices_sol_in_usdc() {
        // Orca SOL/USDC: token A is SOL (9 decimals), token B is USDC (6).
        let sqrt = u("6131710014734520405");
        let price = price_from_sqrt(sqrt, 64, true, 9, 6).unwrap();
        assert!(price > dec!(110) && price < dec!(111), "{price}");
    }

    #[test]
    fn degenerate_inputs_have_no_price() {
        assert_eq!(price_from_sqrt(U256::ZERO, 96, true, 18, 18), None);
        assert_eq!(price_from_sqrt(U256::MAX, 96, true, 18, 0), None);
    }
}
