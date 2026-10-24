//! Schedule expressions: `rate(...)`, `at(...)` and six-field AWS `cron(...)`, with next-fire
//! computation in an IANA time zone.
use jiff::civil::{Date, DateTime, Weekday};
use jiff::tz::TimeZone;

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// Fixed interval in seconds.
    Rate(i64),
    At(DateTime),
    Cron(Box<Cron>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Cron {
    minutes: Vec<u32>,
    hours: Vec<u32>,
    dom: Option<Vec<DomItem>>,
    months: Vec<u32>,
    dow: Option<Vec<DowItem>>,
    years: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq)]
enum DomItem {
    Days(Vec<u32>),
    Last,
    LastWeekday,
    NearestWeekday(u32),
}

#[derive(Debug, Clone, PartialEq)]
enum DowItem {
    /// Days of the week, 1 = Sunday .. 7 = Saturday.
    Days(Vec<u32>),
    /// The last given weekday of the month (`6L`).
    Last(u32),
    /// The nth given weekday of the month (`6#3`).
    Nth(u32, u32),
}

const MONTH_NAMES: &[&str] = &[
    "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC",
];
const DAY_NAMES: &[&str] = &["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"];

pub fn parse(text: &str) -> Result<Expr, String> {
    let text = text.trim();
    let inner = |prefix: &str| {
        text.strip_prefix(prefix)
            .and_then(|r| r.strip_suffix(')'))
            .map(str::trim)
    };
    if let Some(body) = inner("rate(") {
        return parse_rate(body);
    }
    if let Some(body) = inner("at(") {
        return body
            .parse::<DateTime>()
            .map(Expr::At)
            .map_err(|_| format!("Invalid at() expression: {text}"));
    }
    if let Some(body) = inner("cron(") {
        return parse_cron(body).map(|c| Expr::Cron(Box::new(c)));
    }
    Err(format!("Invalid schedule expression: {text}"))
}

fn parse_rate(body: &str) -> Result<Expr, String> {
    let bad = || format!("Invalid rate expression: rate({body})");
    let (value, unit) = body.split_once(' ').ok_or_else(bad)?;
    let value: i64 = value.trim().parse().map_err(|_| bad())?;
    if value < 1 {
        return Err(bad());
    }
    let unit = match unit.trim() {
        "minute" | "minutes" => 60,
        "hour" | "hours" => 3600,
        "day" | "days" => 86_400,
        _ => return Err(bad()),
    };
    Ok(Expr::Rate(value * unit))
}

fn number(text: &str, names: &[&str], base: u32, min: u32, max: u32) -> Result<u32, String> {
    let value = match text.parse::<u32>() {
        Ok(v) => v,
        Err(_) => names
            .iter()
            .position(|n| n.eq_ignore_ascii_case(text))
            .map(|i| i as u32 + base)
            .ok_or_else(|| format!("Invalid value in cron expression: {text}"))?,
    };
    if !(min..=max).contains(&value) {
        return Err(format!("Value {value} is out of range {min}-{max}"));
    }
    Ok(value)
}

/// One comma-separated element: `*`, `a`, `a-b`, `a/s`, `a-b/s`, `*/s`.
fn range_item(
    item: &str,
    names: &[&str],
    base: u32,
    min: u32,
    max: u32,
) -> Result<Vec<u32>, String> {
    let (range, step) = match item.split_once('/') {
        Some((r, s)) => (
            r,
            Some(
                s.parse::<u32>()
                    .ok()
                    .filter(|s| *s > 0)
                    .ok_or_else(|| format!("Invalid step in cron expression: {item}"))?,
            ),
        ),
        None => (item, None),
    };
    let (lo, hi) = if range == "*" {
        (min, max)
    } else if let Some((a, b)) = range.split_once('-') {
        (
            number(a, names, base, min, max)?,
            number(b, names, base, min, max)?,
        )
    } else {
        let a = number(range, names, base, min, max)?;
        (a, if step.is_some() { max } else { a })
    };
    if lo > hi {
        return Err(format!("Invalid range in cron expression: {item}"));
    }
    Ok((lo..=hi).step_by(step.unwrap_or(1) as usize).collect())
}

fn plain_field(
    field: &str,
    names: &[&str],
    base: u32,
    min: u32,
    max: u32,
) -> Result<Vec<u32>, String> {
    let mut out = Vec::new();
    for item in field.split(',') {
        out.extend(range_item(item, names, base, min, max)?);
    }
    out.sort_unstable();
    out.dedup();
    Ok(out)
}

fn dom_field(field: &str) -> Result<Option<Vec<DomItem>>, String> {
    if field == "?" {
        return Ok(None);
    }
    let mut items = Vec::new();
    for item in field.split(',') {
        items.push(match item {
            "L" => DomItem::Last,
            "LW" => DomItem::LastWeekday,
            _ => match item.strip_suffix('W') {
                Some(day) => DomItem::NearestWeekday(number(day, &[], 0, 1, 31)?),
                None => DomItem::Days(range_item(item, &[], 0, 1, 31)?),
            },
        });
    }
    Ok(Some(items))
}

fn dow_field(field: &str) -> Result<Option<Vec<DowItem>>, String> {
    if field == "?" {
        return Ok(None);
    }
    let mut items = Vec::new();
    for item in field.split(',') {
        items.push(if item == "L" {
            DowItem::Days(vec![7])
        } else if let Some((day, nth)) = item.split_once('#') {
            let nth = nth
                .parse::<u32>()
                .ok()
                .filter(|n| (1..=5).contains(n))
                .ok_or_else(|| format!("Invalid occurrence in cron expression: {item}"))?;
            DowItem::Nth(number(day, DAY_NAMES, 1, 1, 7)?, nth)
        } else if let Some(day) = item.strip_suffix('L') {
            DowItem::Last(number(day, DAY_NAMES, 1, 1, 7)?)
        } else {
            DowItem::Days(range_item(item, DAY_NAMES, 1, 1, 7)?)
        });
    }
    Ok(Some(items))
}

fn parse_cron(body: &str) -> Result<Cron, String> {
    let fields: Vec<&str> = body.split_whitespace().collect();
    let [minutes, hours, dom, months, dow, years] = fields[..] else {
        return Err(format!(
            "Invalid cron expression: cron({body}) must have six fields"
        ));
    };
    let cron = Cron {
        minutes: plain_field(minutes, &[], 0, 0, 59)?,
        hours: plain_field(hours, &[], 0, 0, 23)?,
        dom: dom_field(dom)?,
        months: plain_field(months, MONTH_NAMES, 1, 1, 12)?,
        dow: dow_field(dow)?,
        years: plain_field(years, &[], 0, 1970, 2199)?,
    };
    // AWS requires exactly one of day-of-month and day-of-week to be `?`.
    if cron.dom.is_some() == cron.dow.is_some() {
        return Err(
            "Invalid cron expression: day-of-month and day-of-week cannot both be specified, \
             use ? in one of them"
                .into(),
        );
    }
    Ok(cron)
}

impl Cron {
    fn matches_day(&self, date: Date) -> bool {
        if !self.months.contains(&(date.month() as u32))
            || !self.years.contains(&(date.year() as u32))
        {
            return false;
        }
        let day = date.day() as u32;
        let last = date.days_in_month() as u32;
        let weekday = |d: u32| {
            // 0 = Monday .. 4 = Friday
            Date::new(date.year(), date.month(), d as i8)
                .map(|d| d.weekday().to_monday_zero_offset() < 5)
                .unwrap_or(false)
        };
        if let Some(items) = &self.dom {
            return items.iter().any(|item| match item {
                DomItem::Days(days) => days.contains(&day),
                DomItem::Last => day == last,
                DomItem::LastWeekday => {
                    let target = (1..=last).rev().find(|d| weekday(*d)).unwrap_or(last);
                    day == target
                }
                DomItem::NearestWeekday(n) => {
                    // The nearest weekday stays inside the month.
                    let n = (*n).min(last);
                    let target = if weekday(n) {
                        n
                    } else {
                        let wd = Date::new(date.year(), date.month(), n as i8)
                            .unwrap()
                            .weekday();
                        match wd {
                            Weekday::Saturday if n > 1 => n - 1,
                            Weekday::Saturday => n + 2,
                            _ if n < last => n + 1,
                            _ => n - 2,
                        }
                    };
                    day == target
                }
            });
        }
        let dow = date.weekday().to_sunday_one_offset() as u32;
        self.dow.as_ref().is_some_and(|items| {
            items.iter().any(|item| match item {
                DowItem::Days(days) => days.contains(&dow),
                DowItem::Last(d) => dow == *d && day + 7 > last,
                DowItem::Nth(d, n) => dow == *d && (day - 1) / 7 + 1 == *n,
            })
        })
    }
}

/// Schedule bounds and zone shared by next-fire computations.
pub struct Window<'a> {
    pub tz: &'a TimeZone,
    pub start: Option<i64>,
    pub end: Option<i64>,
    /// First firing instant of a `rate` schedule, in epoch seconds.
    pub anchor: i64,
}

/// The first instant strictly after `after` (epoch seconds) at which `expr` fires, within the
/// window. Instants skipped by downtime are never returned: callers pass the current time.
pub fn next_after(expr: &Expr, window: &Window, after: i64) -> Option<i64> {
    let after = window.start.map_or(after, |s| after.max(s - 1));
    let next = match expr {
        Expr::Rate(interval) => {
            if after < window.anchor {
                window.anchor
            } else {
                window.anchor + ((after - window.anchor) / interval + 1) * interval
            }
        }
        Expr::At(at) => {
            let at = at.to_zoned(window.tz.clone()).ok()?.timestamp().as_second();
            if at > after { at } else { return None }
        }
        Expr::Cron(cron) => next_cron(cron, window.tz, after)?,
    };
    window.end.is_none_or(|end| next <= end).then_some(next)
}

fn next_cron(cron: &Cron, tz: &TimeZone, after: i64) -> Option<i64> {
    let start = jiff::Timestamp::from_second(after)
        .ok()?
        .to_zoned(tz.clone());
    let mut date = start.date();
    // Leap-day schedules recur every four years (every eight across a skipped century year).
    for _ in 0..(366 * 9) {
        if cron.matches_day(date) {
            for &hour in &cron.hours {
                for &minute in &cron.minutes {
                    // `to_zoned` resolves DST gaps forward and folds to the earlier instant.
                    let at = date.at(hour as i8, minute as i8, 0, 0).to_zoned(tz.clone());
                    if let Ok(at) = at {
                        let at = at.timestamp().as_second();
                        if at > after {
                            return Some(at);
                        }
                    }
                }
            }
        }
        date = date.tomorrow().ok()?;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(text: &str) -> i64 {
        text.parse::<jiff::Timestamp>().unwrap().as_second()
    }
    fn next(expr: &str, zone: &str, after: &str) -> Option<String> {
        let tz = TimeZone::get(zone).unwrap();
        let window = Window {
            tz: &tz,
            start: None,
            end: None,
            anchor: 0,
        };
        next_after(&parse(expr).unwrap(), &window, ts(after))
            .map(|t| jiff::Timestamp::from_second(t).unwrap().to_string())
    }

    #[test]
    fn parses_valid_and_rejects_invalid_expressions() {
        for ok in [
            "rate(5 minutes)",
            "rate(1 minutes)",
            "rate(1 hour)",
            "cron(0 2 * * ? *)",
            "cron(0/15 8-17 ? * MON-FRI *)",
            "cron(0 12 L * ? 2026-2030)",
            "cron(0 12 15W * ? *)",
            "cron(0 12 ? * 6#3 *)",
            "cron(0 12 ? * 6L *)",
            "at(2026-12-31T23:59:00)",
        ] {
            assert!(parse(ok).is_ok(), "{ok}");
        }
        for bad in [
            "rate(0 minutes)",
            "rate(5 weeks)",
            "rate(minutes)",
            "cron(0 2 * * *)",
            "cron(0 2 * * * *)",
            "cron(61 2 * * ? *)",
            "cron(0 25 * * ? *)",
            "cron(0 2 ? * ? *)",
            "cron(0 2 * * MONDAY *)",
            "at(tomorrow)",
            "every day",
        ] {
            assert!(parse(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn rate_follows_its_anchor_without_backfill() {
        let tz = TimeZone::UTC;
        let window = Window {
            tz: &tz,
            start: None,
            end: None,
            anchor: 1000,
        };
        let rate = Expr::Rate(60);
        assert_eq!(next_after(&rate, &window, 0), Some(1000));
        assert_eq!(next_after(&rate, &window, 1000), Some(1060));
        // Downtime of ten intervals skips straight to the next future tick.
        assert_eq!(next_after(&rate, &window, 1630), Some(1660));
    }

    #[test]
    fn cron_in_utc() {
        assert_eq!(
            next("cron(0 2 * * ? *)", "UTC", "2026-03-01T02:00:00Z").as_deref(),
            Some("2026-03-02T02:00:00Z")
        );
        assert_eq!(
            next("cron(0/15 * * * ? *)", "UTC", "2026-03-01T10:07:00Z").as_deref(),
            Some("2026-03-01T10:15:00Z")
        );
        // Mon-Fri only: Saturday 2026-03-07 rolls to Monday.
        assert_eq!(
            next("cron(0 9 ? * MON-FRI *)", "UTC", "2026-03-06T09:00:00Z").as_deref(),
            Some("2026-03-09T09:00:00Z")
        );
        assert_eq!(
            next("cron(0 0 L * ? *)", "UTC", "2026-02-01T00:00:00Z").as_deref(),
            Some("2026-02-28T00:00:00Z")
        );
        // 2026-03-15 is a Sunday, so 15W fires on Monday the 16th.
        assert_eq!(
            next("cron(0 0 15W * ? *)", "UTC", "2026-03-01T00:00:00Z").as_deref(),
            Some("2026-03-16T00:00:00Z")
        );
        // Third Friday of March 2026.
        assert_eq!(
            next("cron(0 0 ? * 6#3 *)", "UTC", "2026-03-01T00:00:00Z").as_deref(),
            Some("2026-03-20T00:00:00Z")
        );
        assert_eq!(
            next("cron(0 0 ? * 6L *)", "UTC", "2026-03-01T00:00:00Z").as_deref(),
            Some("2026-03-27T00:00:00Z")
        );
        assert_eq!(
            next("cron(0 0 29 2 ? *)", "UTC", "2026-03-01T00:00:00Z").as_deref(),
            Some("2028-02-29T00:00:00Z")
        );
        assert_eq!(
            next("cron(0 0 1 1 ? 2020)", "UTC", "2026-03-01T00:00:00Z"),
            None
        );
    }

    #[test]
    fn cron_keeps_local_time_across_helsinki_dst() {
        // EET (+02:00) until 03:00 on 2026-03-29, EEST (+03:00) after.
        for (after, expected) in [
            ("2026-03-27T00:00:00Z", "2026-03-28T00:00:00Z"),
            ("2026-03-28T00:00:00Z", "2026-03-29T00:00:00Z"),
            ("2026-03-29T00:00:00Z", "2026-03-29T23:00:00Z"),
        ] {
            assert_eq!(
                next("cron(0 2 * * ? *)", "Europe/Helsinki", after).as_deref(),
                Some(expected)
            );
        }
        // Autumn change on 2026-10-25.
        assert_eq!(
            next(
                "cron(0 2 * * ? *)",
                "Europe/Helsinki",
                "2026-10-24T23:00:00Z"
            )
            .as_deref(),
            Some("2026-10-26T00:00:00Z")
        );
    }

    #[test]
    fn dst_gap_resolves_forward_and_fold_fires_once() {
        // 03:30 does not exist in Helsinki on 2026-03-29 (03:00 -> 04:00).
        assert_eq!(
            next(
                "cron(30 3 * * ? *)",
                "Europe/Helsinki",
                "2026-03-28T23:00:00Z"
            )
            .as_deref(),
            Some("2026-03-29T01:30:00Z")
        );
        // 03:30 happens twice on 2026-10-25; only the first (EEST) instant fires.
        assert_eq!(
            next(
                "cron(30 3 * * ? *)",
                "Europe/Helsinki",
                "2026-10-24T23:00:00Z"
            )
            .as_deref(),
            Some("2026-10-25T00:30:00Z")
        );
        assert_eq!(
            next(
                "cron(30 3 * * ? *)",
                "Europe/Helsinki",
                "2026-10-25T00:30:00Z"
            )
            .as_deref(),
            Some("2026-10-26T01:30:00Z")
        );
    }

    #[test]
    fn at_fires_once_in_its_zone() {
        assert_eq!(
            next(
                "at(2026-12-31T23:59:00)",
                "Europe/Helsinki",
                "2026-01-01T00:00:00Z"
            )
            .as_deref(),
            Some("2026-12-31T21:59:00Z")
        );
        assert_eq!(
            next("at(2026-12-31T23:59:00)", "UTC", "2027-01-01T00:00:00Z"),
            None
        );
    }

    #[test]
    fn start_and_end_dates_bound_firing() {
        let tz = TimeZone::UTC;
        let expr = parse("cron(0 * * * ? *)").unwrap();
        let window = Window {
            tz: &tz,
            start: Some(ts("2026-06-01T00:00:00Z")),
            end: Some(ts("2026-06-01T02:00:00Z")),
            anchor: 0,
        };
        let at =
            |after| next_after(&expr, &window, ts(after)).map(|t| t - ts("2026-06-01T00:00:00Z"));
        assert_eq!(at("2026-01-01T00:00:00Z"), Some(0));
        assert_eq!(at("2026-06-01T00:00:00Z"), Some(3600));
        assert_eq!(at("2026-06-01T01:00:00Z"), Some(7200));
        assert_eq!(at("2026-06-01T02:00:00Z"), None);
    }
}
