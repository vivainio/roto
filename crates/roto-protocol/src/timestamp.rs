/// Seconds since the Unix epoch, rendered as ISO-8601 on the wire.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct Timestamp(pub i64);

impl Timestamp {
    pub fn now() -> Self {
        let d = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        Self(d.as_secs() as i64)
    }

    /// `2012-01-01T12:00:00Z`
    pub fn to_iso8601(self) -> String {
        let days = self.0.div_euclid(86_400);
        let secs = self.0.rem_euclid(86_400);
        let (y, m, d) = civil_from_days(days);
        format!(
            "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z",
            secs / 3600,
            secs % 3600 / 60,
            secs % 60
        )
    }
}

/// Howard Hinnant's days-to-civil algorithm.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = yoe + era * 400 + i64::from(m <= 2);
    (y, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_iso8601() {
        assert_eq!(Timestamp(0).to_iso8601(), "1970-01-01T00:00:00Z");
        assert_eq!(
            Timestamp(1_325_419_200).to_iso8601(),
            "2012-01-01T12:00:00Z"
        );
        assert_eq!(
            Timestamp(1_709_164_800).to_iso8601(),
            "2024-02-29T00:00:00Z"
        );
        assert_eq!(Timestamp(-1).to_iso8601(), "1969-12-31T23:59:59Z");
    }
}
