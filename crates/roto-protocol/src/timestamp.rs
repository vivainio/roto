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

const DAYS: [&str; 7] = ["Thu", "Fri", "Sat", "Sun", "Mon", "Tue", "Wed"];
const MONTHS: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

impl Timestamp {
    /// `2012-01-01T12:00:00.000Z` (S3 style, always three fractional digits).
    pub fn to_iso8601_millis(self) -> String {
        let s = self.to_iso8601();
        format!("{}.000Z", &s[..s.len() - 1])
    }

    /// RFC 7231 HTTP date: `Sun, 01 Jan 2012 12:00:00 GMT`.
    pub fn to_http_date(self) -> String {
        let days = self.0.div_euclid(86_400);
        let secs = self.0.rem_euclid(86_400);
        let (y, m, d) = civil_from_days(days);
        format!(
            "{}, {d:02} {} {y:04} {:02}:{:02}:{:02} GMT",
            DAYS[days.rem_euclid(7) as usize],
            MONTHS[m as usize - 1],
            secs / 3600,
            secs % 3600 / 60,
            secs % 60
        )
    }

    /// Accepts an HTTP date, ISO-8601 (with optional fraction / `Z`), or epoch seconds.
    pub fn parse(s: &str) -> Option<Self> {
        let s = s.trim();
        if let Ok(n) = s.parse::<f64>() {
            return Some(Self(n as i64));
        }
        parse_iso8601(s).or_else(|| parse_http_date(s))
    }
}

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn parse_iso8601(s: &str) -> Option<Timestamp> {
    let (date, time) = s.split_once('T')?;
    let mut d = date.split('-');
    let (y, mo, da): (i64, i64, i64) = (
        d.next()?.parse().ok()?,
        d.next()?.parse().ok()?,
        d.next()?.parse().ok()?,
    );
    let time = time.trim_end_matches('Z');
    let time = time.split(['+', '.']).next()?;
    let mut t = time.split(':');
    let (h, mi, se): (i64, i64, i64) = (
        t.next()?.parse().ok()?,
        t.next()?.parse().ok()?,
        t.next().unwrap_or("0").parse().ok()?,
    );
    Some(Timestamp(
        days_from_civil(y, mo, da) * 86_400 + h * 3600 + mi * 60 + se,
    ))
}

fn parse_http_date(s: &str) -> Option<Timestamp> {
    // `Sun, 01 Jan 2012 12:00:00 GMT`
    let rest = s.split_once(", ").map_or(s, |(_, r)| r);
    let mut p = rest.split_whitespace();
    let day: i64 = p.next()?.parse().ok()?;
    let mon_name = p.next()?;
    let mon = MONTHS
        .iter()
        .position(|m| m.eq_ignore_ascii_case(mon_name))? as i64
        + 1;
    let year: i64 = p.next()?.parse().ok()?;
    let mut t = p.next()?.split(':');
    let (h, mi, se): (i64, i64, i64) = (
        t.next()?.parse().ok()?,
        t.next()?.parse().ok()?,
        t.next()?.parse().ok()?,
    );
    Some(Timestamp(
        days_from_civil(year, mon, day) * 86_400 + h * 3600 + mi * 60 + se,
    ))
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

    #[test]
    fn http_dates_and_parsing_round_trip() {
        let t = Timestamp(1_325_419_200);
        assert_eq!(t.to_http_date(), "Sun, 01 Jan 2012 12:00:00 GMT");
        assert_eq!(t.to_iso8601_millis(), "2012-01-01T12:00:00.000Z");
        for s in [
            "Sun, 01 Jan 2012 12:00:00 GMT",
            "2012-01-01T12:00:00.000Z",
            "2012-01-01T12:00:00Z",
            "1325419200",
        ] {
            assert_eq!(Timestamp::parse(s), Some(t), "{s}");
        }
        assert_eq!(Timestamp(0).to_http_date(), "Thu, 01 Jan 1970 00:00:00 GMT");
        assert!(Timestamp::parse("garbage").is_none());
    }
}
