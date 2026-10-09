//! DynamoDB numbers: up to 38 significant digits, magnitude between 1E-130 and 9.99…E125,
//! carried over the wire as strings and compared/added as decimals.

use std::cmp::Ordering;
use std::str::FromStr;

use bigdecimal::num_bigint::Sign;
use bigdecimal::{BigDecimal, Zero};

pub type Number = BigDecimal;

#[derive(Debug, PartialEq, Eq)]
pub enum NumberError {
    Invalid,
    /// More than 38 significant digits.
    TooPrecise,
    OutOfRange,
}

/// Parses and validates a DynamoDB number string (`"12"`, `"-0.5"`, `"1.2E+3"`).
pub fn parse(s: &str) -> Result<Number, NumberError> {
    let t = s.trim();
    if t.is_empty()
        || t.contains(['_', ' '])
        || t.eq_ignore_ascii_case("nan")
        || t.to_ascii_lowercase().contains("inf")
    {
        return Err(NumberError::Invalid);
    }
    let n = BigDecimal::from_str(t).map_err(|_| NumberError::Invalid)?;
    if n.is_zero() {
        return Ok(BigDecimal::from(0));
    }
    let n = n.normalized();
    let (digits, _) = n.as_bigint_and_exponent();
    let digit_count = digits.magnitude().to_string().len();
    if digit_count > 38 {
        return Err(NumberError::TooPrecise);
    }
    let e = exponent10(&n);
    if !(-130..=125).contains(&e) {
        return Err(NumberError::OutOfRange);
    }
    Ok(n)
}

/// Position of the leading digit: `123.4` -> 2, `0.05` -> -2.
fn exponent10(n: &Number) -> i64 {
    let (digits, scale) = n.as_bigint_and_exponent();
    digits.magnitude().to_string().len() as i64 - scale - 1
}

/// Canonical text form: no exponent, no trailing zeros (`1E+2` -> `100`, `1.50` -> `1.5`).
pub fn to_string(n: &Number) -> String {
    if n.is_zero() {
        return "0".into();
    }
    let n = n.normalized();
    let (digits, scale) = n.as_bigint_and_exponent();
    let negative = digits.sign() == Sign::Minus;
    let d = digits.magnitude().to_string();
    let body = if scale <= 0 {
        format!("{d}{}", "0".repeat((-scale) as usize))
    } else if (scale as usize) >= d.len() {
        format!("0.{}{d}", "0".repeat(scale as usize - d.len()))
    } else {
        let split = d.len() - scale as usize;
        format!("{}.{}", &d[..split], &d[split..])
    };
    if negative { format!("-{body}") } else { body }
}

pub fn cmp(a: &Number, b: &Number) -> Ordering {
    a.cmp(b)
}

/// Order-preserving bytes: comparing encodings bytewise equals comparing the numbers.
pub fn sort_key(n: &Number) -> Vec<u8> {
    if n.is_zero() {
        return vec![1];
    }
    let n = n.normalized();
    let (digits, _) = n.as_bigint_and_exponent();
    let negative = digits.sign() == Sign::Minus;
    let mut body = Vec::new();
    // Bias so negative exponents sort below positive ones.
    body.extend(((exponent10(&n) + 1000) as u32).to_be_bytes());
    body.extend(digits.magnitude().to_string().bytes());
    body.push(0);
    if negative {
        let mut out = vec![0];
        out.extend(body.iter().map(|b| !b));
        out
    } else {
        let mut out = vec![2];
        out.extend(body);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn n(s: &str) -> Number {
        parse(s).unwrap()
    }

    #[test]
    fn canonical_forms() {
        for (input, want) in [
            ("12", "12"),
            ("1.50", "1.5"),
            ("1E+2", "100"),
            ("-0.0", "0"),
            ("0.000", "0"),
            ("-1.25e-3", "-0.00125"),
            ("100", "100"),
            ("0.5", "0.5"),
        ] {
            assert_eq!(to_string(&n(input)), want, "{input}");
        }
    }

    #[test]
    fn validation() {
        assert_eq!(parse("abc"), Err(NumberError::Invalid));
        assert_eq!(parse(""), Err(NumberError::Invalid));
        assert_eq!(parse("NaN"), Err(NumberError::Invalid));
        assert_eq!(parse(&"1".repeat(39)), Err(NumberError::TooPrecise));
        assert!(parse(&"1".repeat(38)).is_ok());
        assert_eq!(parse("1E126"), Err(NumberError::OutOfRange));
        assert_eq!(parse("1E-131"), Err(NumberError::OutOfRange));
        assert!(parse("9.9999E125").is_ok());
    }

    #[test]
    fn sort_keys_follow_numeric_order() {
        let ordered = [
            "-1000", "-12.5", "-12.25", "-1", "-0.5", "-0.001", "0", "0.001", "0.5", "1", "1.25",
            "12", "99", "100", "1E20",
        ];
        let keys: Vec<_> = ordered.iter().map(|s| sort_key(&n(s))).collect();
        for w in keys.windows(2) {
            assert!(w[0] < w[1], "{:?} !< {:?}", w[0], w[1]);
        }
        assert_eq!(sort_key(&n("1.0")), sort_key(&n("1")));
    }
}
