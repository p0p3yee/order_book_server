//! Exact decimal arithmetic; aggregation never changes the raw-fill subscription.
use super::*;
use num_bigint::BigInt;
use num_traits::{Signed, Zero};
use std::str::FromStr;
#[derive(Clone)]
struct Decimal {
    n: BigInt,
    scale: u32,
}
impl Decimal {
    fn parse(s: &str) -> Result<Self, String> {
        if s.len() > 128 {
            return Err("decimal too long".into());
        }
        let (mantissa, exponent) =
            s.split_once(['e', 'E']).map_or((s, Ok(0)), |(m, e)| (m, e.parse::<i32>().map_err(|_| "invalid exponent")));
        let exponent = exponent?;
        let negative = mantissa.starts_with('-');
        let mantissa = mantissa.strip_prefix(['-', '+']).unwrap_or(mantissa);
        let mut parts = mantissa.split('.');
        let whole = parts.next().unwrap_or("");
        let fraction = parts.next().unwrap_or("");
        if parts.next().is_some()
            || whole.len() + fraction.len() == 0
            || !whole.bytes().chain(fraction.bytes()).all(|b| b.is_ascii_digit())
        {
            return Err("invalid decimal".into());
        }
        let scale = fraction.len() as i32 - exponent;
        if !(-36..=36).contains(&scale) {
            return Err("decimal scale exceeds 36".into());
        }
        let mut n = BigInt::from_str(&format!("{whole}{fraction}")).map_err(|_| "invalid decimal")?;
        if negative {
            n = -n;
        }
        if scale < 0 {
            n *= BigInt::from(10).pow((-scale) as u32);
        }
        Ok(Self { n, scale: scale.max(0) as u32 })
    }
    fn add(&mut self, other: &Self) {
        let scale = self.scale.max(other.scale);
        self.n =
            &self.n * BigInt::from(10).pow(scale - self.scale) + &other.n * BigInt::from(10).pow(scale - other.scale);
        self.scale = scale;
    }
    fn mul(&self, other: &Self) -> Self {
        Self { n: &self.n * &other.n, scale: self.scale + other.scale }
    }
    fn text(&self) -> String {
        let sign = if self.n.is_negative() { "-" } else { "" };
        let mut digits = self.n.abs().to_string();
        if self.scale > 0 {
            if digits.len() <= self.scale as usize {
                digits = format!("{}{}", "0".repeat(self.scale as usize + 1 - digits.len()), digits);
            }
            digits.insert(digits.len() - self.scale as usize, '.');
            while digits.ends_with('0') {
                digits.pop();
            }
            if digits.ends_with('.') {
                digits.pop();
            }
        }
        if self.n.is_zero() { "0".into() } else { format!("{sign}{digits}") }
    }
    fn mean(&self, sz: &Self) -> Result<Self, String> {
        if sz.n <= BigInt::zero() {
            return Err("aggregate quantity must be positive".into());
        }
        let numerator = &self.n * BigInt::from(10).pow(sz.scale + 18);
        let denominator = &sz.n * BigInt::from(10).pow(self.scale);
        let mut n = &numerator / &denominator;
        let remainder = &numerator % &denominator;
        if remainder.abs() * 2 >= denominator.abs() {
            n += if numerator.is_negative() { -1 } else { 1 };
        }
        Ok(Self { n, scale: 18 })
    }
}
fn decimal(v: &Value, k: &str) -> Result<Decimal, String> {
    Decimal::parse(v[k].as_str().ok_or_else(|| format!("missing decimal {k}"))?)
}
struct Group {
    first: Value,
    sz: Decimal,
    notional: Decimal,
    sums: HashMap<&'static str, Decimal>,
    last_seq: u64,
}
/// Group crossing fills by execution hash/order, maker fills by block/order.
/// The first constituent supplies identifiers/startPosition; amount fields are exact
/// sums and px is the size-weighted mean rounded half-away-from-zero to 18 places.
/// Public grouping rules are documented, but public metadata tie-breaking is not.
pub(super) fn fills<'a, I: Iterator<Item = &'a Event>>(
    events: I,
    after: Option<u64>,
    complete_height: u64,
) -> Result<Vec<Value>, String> {
    let mut groups = Vec::<Group>::new();
    let mut index = HashMap::new();
    for event in events {
        if event.height > complete_height {
            continue;
        }
        let f = &event.data;
        let boundary = if event.height == 0 {
            format!("legacy:{}", event.seq)
        } else if f["crossed"] == true {
            format!("t:{}", f["hash"])
        } else {
            format!("m:{}", event.height)
        };
        let key =
            json!([event.user, f["coin"], f["oid"], f["side"], f["feeToken"], boundary, f["liquidation"]]).to_string();
        let size = decimal(f, "sz")?;
        if size.n <= BigInt::zero() {
            return Err("fill quantity must be positive".into());
        }
        let notional = decimal(f, "px")?.mul(&size);
        let sums = ["fee", "closedPnl", "builderFee", "deployerFee"]
            .into_iter()
            .filter(|k| f.get(*k).is_some())
            .map(|k| Ok((k, decimal(f, k)?)))
            .collect::<Result<HashMap<_, _>, String>>()?;
        if let Some(i) = index.get(&key).copied() {
            let g: &mut Group = &mut groups[i];
            g.sz.add(&size);
            g.notional.add(&notional);
            g.last_seq = g.last_seq.max(event.seq);
            for (k, v) in sums {
                g.sums.entry(k).and_modify(|sum| sum.add(&v)).or_insert(v);
            }
        } else {
            index.insert(key, groups.len());
            groups.push(Group { first: f.clone(), sz: size, notional, sums, last_seq: event.seq });
        }
    }
    let mut output = Vec::new();
    for mut g in groups {
        if after.is_some_and(|seq| g.last_seq <= seq) {
            continue;
        }
        g.first["px"] = json!(g.notional.mean(&g.sz)?.text());
        g.first["sz"] = json!(g.sz.text());
        for (k, v) in g.sums {
            g.first[k] = json!(v.text());
        }
        output.push(g.first);
    }
    Ok(output)
}
#[cfg(test)]
mod tests {
    use super::*;
    fn event(seq: u64, height: u64, crossed: bool, hash: &str, px: &str, sz: &str) -> Event {
        let data = json!({"coin":"BTC","oid":1,"side":"B","feeToken":"USDC","crossed":crossed,"hash":hash,"tid":seq,"time":100,"startPosition":"0","dir":"Open Long","px":px,"sz":sz,"fee":"-0.001","closedPnl":"0.02","builderFee":"0.0001","deployerFee":"0.0002"});
        Event { seq, height, user: "wallet".into(), channel: "userFills".into(), data, key: seq.to_string(), bytes: 0 }
    }
    #[test]
    fn exact_weighted_price_fees_and_public_grouping_boundaries() {
        let e = vec![
            event(1, 9, true, "tx", "100.1", "0.1"),
            event(2, 9, true, "tx", "100.3", "0.3"),
            event(3, 9, false, "a", "10", "1"),
            event(4, 9, false, "b", "12", "1"),
            event(5, 10, false, "c", "20", "1"),
        ];
        let out = fills(e.iter(), None, 10).unwrap();
        assert_eq!(out.len(), 3);
        assert_eq!(out[0]["px"], "100.25");
        assert_eq!(out[0]["sz"], "0.4");
        assert_eq!(out[0]["fee"], "-0.002");
        assert_eq!(out[0]["builderFee"], "0.0002");
        assert_eq!(out[0]["deployerFee"], "0.0004");
        assert_eq!(out[1]["px"], "11");
        assert_eq!(out[2]["px"], "20");
        assert_eq!(fills(e.iter(), Some(4), 10).unwrap().len(), 1);
        assert_eq!(fills(e.iter(), None, 9).unwrap().len(), 2);
    }
    #[test]
    fn scientific_values_and_rounding_are_not_floating_point() {
        assert_eq!(Decimal::parse("1e-8").unwrap().text(), "0.00000001");
        assert_eq!(
            Decimal::parse("1").unwrap().mean(&Decimal::parse("3").unwrap()).unwrap().text(),
            "0.333333333333333333"
        );
        assert!(Decimal::parse("NaN").is_err());
        assert!(Decimal::parse("1e100").is_err());
    }
}
