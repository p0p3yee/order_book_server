use crate::prelude::*;
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Formatter};
use std::ops::Add;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub(crate) enum Side {
    #[serde(rename = "A")]
    Ask,
    #[serde(rename = "B")]
    Bid,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct Oid(u64);

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Px(u64);

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Sz(u64);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct Coin(String);

impl Sz {
    pub(crate) const fn new(value: u64) -> Self {
        Self(value)
    }
    pub(super) const fn is_positive(self) -> bool {
        self.0 > 0
    }
    pub(super) const fn is_zero(self) -> bool {
        self.0 == 0
    }
    pub(crate) const fn value(self) -> u64 {
        self.0
    }
    pub(crate) const fn decrement_sz(&mut self, dec: u64) {
        self.0 = self.0.saturating_sub(dec);
    }
}

impl Px {
    pub(crate) const fn new(value: u64) -> Self {
        Self(value)
    }
    pub(crate) const fn value(self) -> u64 {
        self.0
    }
}

impl Oid {
    pub(crate) const fn new(value: u64) -> Self {
        Self(value)
    }
}

pub(crate) trait InnerOrder: Clone {
    fn coin(&self) -> Coin;
    fn oid(&self) -> Oid;
    fn side(&self) -> Side;
    fn limit_px(&self) -> Px;
    fn sz(&self) -> Sz;
    fn decrement_sz(&mut self, dec: Sz);
    fn fill(&mut self, maker_order: &mut Self) -> Sz;
    fn modify_sz(&mut self, sz: Sz);
    fn convert_trigger(&mut self, ts: u64);
}

impl Coin {
    pub(crate) fn new(coin: &str) -> Self {
        Self(coin.to_string())
    }

    pub(crate) fn value(&self) -> String {
        self.0.clone()
    }

    pub(crate) fn is_spot(&self) -> bool {
        self.0.starts_with('@') || self.0 == "PURR/USDC"
    }
}

impl Add<Self> for Sz {
    type Output = Self;

    fn add(self, rhs: Self) -> Self {
        Self(self.0 + rhs.0)
    }
}

// Multiply all sizes and prices by 10^MAX_DECIMALS for ease of computation.
const MULTIPLIER: f64 = 100_000_000.0;

impl Debug for Px {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", (self.value() as f64 / MULTIPLIER))
    }
}

impl Debug for Sz {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", (self.value() as f64 / MULTIPLIER))
    }
}

impl Px {
    #[allow(clippy::cast_possible_truncation)]
    #[allow(clippy::cast_sign_loss)]
    pub(crate) fn parse_from_str(value: &str) -> Result<Self> {
        let value = parse_decimal(value)?;
        Ok(Self::new(value))
    }

    #[must_use]
    pub(crate) fn to_str(self) -> String {
        let s = format!("{}.{:08}", self.value() / 100_000_000, self.value() % 100_000_000);
        let s = s.trim_end_matches('0');
        s.trim_end_matches('.').to_string()
    }

    #[allow(clippy::cast_possible_truncation)]
    #[allow(clippy::cast_sign_loss)]
    pub(crate) fn num_digits(self) -> u32 {
        if self.value() == 0 { 1 } else { (self.value() as f64).log10().floor() as u32 + 1 }
    }
}

impl Sz {
    #[allow(clippy::cast_possible_truncation)]
    #[allow(clippy::cast_sign_loss)]
    pub(crate) fn parse_from_str(value: &str) -> Result<Self> {
        let value = parse_decimal(value)?;
        Ok(Self::new(value))
    }

    #[must_use]
    pub(crate) fn to_str(self) -> String {
        let s = format!("{}.{:08}", self.value() / 100_000_000, self.value() % 100_000_000);
        let s = s.trim_end_matches('0');
        s.trim_end_matches('.').to_string()
    }
}

fn parse_decimal(value: &str) -> Result<u64> {
    let (whole, frac) = value.split_once('.').unwrap_or((value, ""));
    if whole.is_empty()
        || !whole.bytes().all(|c| c.is_ascii_digit())
        || !frac.bytes().all(|c| c.is_ascii_digit())
        || (frac.len() > 8 && frac[8..].bytes().any(|c| c != b'0'))
    {
        return Err("invalid fixed point decimal".into());
    }
    let frac = &frac[..frac.len().min(8)];
    let fraction = if frac.is_empty() { 0 } else { frac.parse::<u64>()? * 10_u64.pow(8 - frac.len() as u32) };
    whole
        .parse::<u64>()?
        .checked_mul(100_000_000)
        .and_then(|v| v.checked_add(fraction))
        .ok_or_else(|| "decimal overflow".into())
}

#[cfg(test)]
mod decimal_tests {
    use super::*;
    #[test]
    fn exact_prices_and_invalid_numbers() {
        for v in ["100.00000001", "99999999.99999999", "0.00000001"] {
            assert_eq!(Px::parse_from_str(v).unwrap().to_str(), v);
        }
        for v in ["NaN", "inf", "-1", "1e3", "0.000000001", "99999999999999999999999"] {
            assert!(Px::parse_from_str(v).is_err());
        }
    }
}
