//! Dates saisies par un modèle ou une personne : `2026-09-30`, `2026-09-30T08:00`,
//! RFC 3339, `today`, `yesterday`, `aujourd'hui`, `hier`, ou une durée écoulée
//! (`24h`, `7d`, `30m`, `2w`).

use jiff::civil::{Date, DateTime};
use jiff::tz::TimeZone;
use jiff::{Timestamp, ToSpan, Zoned};

/// Instant en millisecondes. `end_of_day` : une date seule vaut sa dernière
/// milliseconde (pour une borne `until`), sinon son début.
pub fn parse(input: &str, tz: &TimeZone, now: Timestamp, end_of_day: bool) -> Result<i64, String> {
    let s = input.trim().to_lowercase();
    let bad = || {
        format!(
            "date illisible {input:?} : attendu 2026-09-30, 2026-09-30T08:00, today, yesterday ou une durée (24h, 7d, 30m, 2w)"
        )
    };
    let today = now.to_zoned(tz.clone()).date();
    let day = |d: Date| -> Result<i64, String> {
        let start = d.to_zoned(tz.clone()).map_err(|e| e.to_string())?;
        let at = if end_of_day {
            start
                .checked_add(1.day())
                .map_err(|e| e.to_string())?
                .timestamp()
                .as_millisecond()
                - 1
        } else {
            start.timestamp().as_millisecond()
        };
        Ok(at)
    };
    match s.as_str() {
        "today" | "aujourd'hui" | "aujourdhui" => return day(today),
        "yesterday" | "hier" => return day(today.yesterday().map_err(|e| e.to_string())?),
        _ => {}
    }
    if let Some((n, unit)) = split_duration(&s) {
        let span = match unit {
            "m" | "min" => n.minutes(),
            "h" => n.hours(),
            "d" | "j" => n.days(),
            "w" | "sem" => n.weeks(),
            _ => return Err(bad()),
        };
        let at = now
            .to_zoned(tz.clone())
            .checked_sub(span)
            .map_err(|e| e.to_string())?;
        return Ok(at.timestamp().as_millisecond());
    }
    if let Ok(ts) = s.parse::<Timestamp>() {
        return Ok(ts.as_millisecond());
    }
    if let Ok(z) = s.parse::<Zoned>() {
        return Ok(z.timestamp().as_millisecond());
    }
    if let Ok(dt) = s.parse::<DateTime>() {
        return dt
            .to_zoned(tz.clone())
            .map(|z| z.timestamp().as_millisecond())
            .map_err(|e| e.to_string());
    }
    if let Ok(d) = s.parse::<Date>() {
        return day(d);
    }
    Err(bad())
}

fn split_duration(s: &str) -> Option<(i64, &str)> {
    let idx = s.find(|c: char| !c.is_ascii_digit())?;
    let (n, unit) = s.split_at(idx);
    let unit = unit.trim();
    // `2026-09-30` commence aussi par des chiffres : une durée n'a que des lettres ensuite.
    if unit.is_empty() || !unit.chars().all(char::is_alphabetic) {
        return None;
    }
    Some((n.parse().ok()?, unit))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(s: &str) -> i64 {
        let tz = TimeZone::get("Indian/Reunion").expect("fuseau");
        let now: Timestamp = "2026-09-30T08:00:00Z".parse().expect("instant");
        parse(s, &tz, now, false).expect(s)
    }

    #[test]
    fn formats() {
        let now = 1_790_755_200_000; // 2026-09-30T08:00:00Z
        assert_eq!(at("24h"), now - 86_400_000);
        assert_eq!(at("30m"), now - 1_800_000);
        // Minuit à La Réunion (UTC+4) = 20:00 UTC la veille.
        assert_eq!(at("2026-09-30"), at("2026-09-29T20:00:00Z"));
        assert_eq!(at("today"), at("2026-09-30"));
        assert_eq!(at("hier"), at("2026-09-29"));
        assert!(parse("demain peut-être", &TimeZone::UTC, Timestamp::now(), false).is_err());
    }
}
