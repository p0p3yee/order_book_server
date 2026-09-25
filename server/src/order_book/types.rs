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
    // Accept the decimal/scientific notation accepted by the reference reader,
    // but never round a value that the book's eight-decimal representation cannot hold.
    let value = value.strip_prefix('+').unwrap_or(value);
    let mut parts = value.split(['e', 'E']);
    let mantissa = parts.next().ok_or("missing decimal mantissa")?;
    let exponent = parts.next().map(str::parse::<i64>).transpose()?.unwrap_or(0);
    if parts.next().is_some() {
        return Err("multiple decimal exponents".into());
    }
    let (whole, frac) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if (whole.is_empty() && frac.is_empty())
        || !whole.bytes().all(|c| c.is_ascii_digit())
        || !frac.bytes().all(|c| c.is_ascii_digit())
    {
        return Err("invalid nonnegative decimal".into());
    }
    let digits = format!("{whole}{frac}");
    let digits = digits.trim_start_matches('0');
    if digits.is_empty() {
        return Ok(0);
    }
    let shift = exponent
        .checked_add(8)
        .and_then(|v| v.checked_sub(frac.len() as i64))
        .ok_or("decimal exponent out of range")?;
    let (digits, zeros) = if shift < 0 {
        let remove = shift.unsigned_abs();
        if remove > digits.len() as u64 {
            return Err("decimal is not exactly representable at eight decimal places".into());
        }
        let split = digits.len() - remove as usize;
        if digits.as_bytes()[split..].iter().any(|b| *b != b'0') {
            return Err("decimal is not exactly representable at eight decimal places".into());
        }
        (&digits[..split], 0)
    } else {
        (digits, shift as u64)
    };
    if digits.len() as u64 + zeros > 20 {
        return Err("decimal overflow".into());
    }
    let mut result = digits.parse::<u64>()?;
    for _ in 0..zeros {
        result = result.checked_mul(10).ok_or("decimal overflow")?;
    }
    Ok(result)
}

#[cfg(test)]
mod decimal_tests {
    use super::*;
    #[test]
    fn scientific_notation_is_exact() {
        for (input, expected) in [
            ("1e-8", "0.00000001"),
            ("1.234e2", "123.4"),
            ("1E+3", "1000"),
            ("+0.50", "0.5"),
            (".5", "0.5"),
            ("10e-9", "0.00000001"),
            ("0e-100", "0"),
            ("184467440737.09551615", "184467440737.09551615"),
        ] {
            assert_eq!(Px::parse_from_str(input).unwrap().to_str(), expected);
            assert_eq!(Sz::parse_from_str(input).unwrap().to_str(), expected);
        }
        for input in [
            "1e-9",
            "1.234567891",
            "1e100",
            "1e-100",
            "1e",
            "1e2e3",
            "-1e-8",
            "184467440737.09551616",
            "1e9223372036854775807",
            "1e-9223372036854775808",
        ] {
            assert!(Px::parse_from_str(input).is_err(), "accepted {input}");
        }
    }
    #[test]
    fn exact_prices_and_invalid_numbers() {
        for v in ["100.00000001", "99999999.99999999", "0.00000001"] {
            assert_eq!(Px::parse_from_str(v).unwrap().to_str(), v);
        }
        for v in ["NaN", "inf", "-1", "0.000000001", "99999999999999999999999"] {
            assert!(Px::parse_from_str(v).is_err());
        }
    }
}
