//! Explicit source inspection. Background warnings stay concise and ephemeral.
use crate::{
    protocol::SourceState,
    sources::{SourceCoverage, SourceScanPhase, SourceScanUnit},
};

fn local_time(milliseconds: Option<u64>) -> Option<String> {
    milliseconds
        .filter(|value| *value > 0)
        .and_then(|value| i64::try_from(value).ok())
        .and_then(chrono::DateTime::from_timestamp_millis)
        .map(|value| {
            value
                .with_timezone(&chrono::Local)
                .format("%Y-%m-%d %H:%M:%S %Z")
                .to_string()
        })
}

pub fn format(sources: &[SourceState]) -> String {
    if sources.is_empty() {
        return "No source checks recorded.".into();
    }
    sources
        .iter()
        .map(|source| {
            let (status, diagnostic) = match &source.coverage {
                SourceCoverage::Complete => ("current", None),
                SourceCoverage::More if source.last_completed_at.is_some() => ("refreshing", None),
                SourceCoverage::More => ("catching up", None),
                SourceCoverage::Partial { message } => ("partial", Some(message)),
                SourceCoverage::Unavailable { message } => ("unavailable", Some(message)),
            };
            let checked =
                local_time(Some(source.checked_at)).unwrap_or_else(|| "not checked".into());
            let completed =
                local_time(source.last_completed_at).unwrap_or_else(|| "not recorded".into());
            let mut text = format!(
                "{} / {}: {status}\nLast checked: {checked}\nLast completed scan: {completed}",
                source.source.provider, source.source.account
            );
            if let Some(progress) = &source.progress {
                let phase = match progress.phase {
                    SourceScanPhase::Discovering => "Discovering",
                    SourceScanPhase::Reading => "Reading",
                };
                let unit = match progress.unit {
                    SourceScanUnit::Calendars => "calendars",
                    SourceScanUnit::Chats => "chats",
                    SourceScanUnit::Messages => "messages",
                };
                text.push_str(&format!("\n{phase} {unit}"));
                if let Some(completed) = progress.completed {
                    text.push_str(&format!(": {completed}"));
                }
                if let Some(total) = progress.total {
                    text.push_str(&format!(" / {total}"));
                }
                let start = local_time(progress.window_start.checked_mul(1000))
                    .unwrap_or_else(|| "unknown".into());
                let end = local_time(progress.window_end.checked_mul(1000))
                    .unwrap_or_else(|| "unknown".into());
                text.push_str(&format!("\nScan window: {start} to {end}"));
            }
            if let Some(diagnostic) = diagnostic {
                text.push_str("\nError: ");
                text.push_str(diagnostic);
            }
            text
        })
        .collect::<Vec<_>>()
        .join("\n\n")
}

#[cfg(test)]
mod tests {
    use crate::{
        protocol::SourceState,
        sources::{SourceScanPhase, SourceScanProgress, SourceScanUnit},
    };

    #[test]
    fn legacy_status_has_no_invented_completion_and_refresh_exposes_safe_scan_progress() {
        let mut state: SourceState = serde_json::from_str(r#"{"source":{"provider":"google_calendar","account":"fixture"},"checkpoint":null,"coverage":{"status":"more"},"checked_at":1800000000000}"#).unwrap();
        assert!(state.last_completed_at.is_none());
        assert!(state.progress.is_none());
        let old = crate::source_view::format(&[state.clone()]);
        assert!(old.contains("catching up"));
        assert!(old.contains("Last completed scan: not recorded"));
        state.last_completed_at = Some(1_799_999_000_000);
        state.progress = Some(SourceScanProgress {
            window_start: 1_799_913_600,
            window_end: 1_802_592_000,
            phase: SourceScanPhase::Reading,
            unit: SourceScanUnit::Calendars,
            completed: Some(8),
            total: Some(9),
        });
        let refreshing = crate::source_view::format(&[state]);
        assert!(refreshing.contains("refreshing"));
        assert!(refreshing.contains("Reading calendars: 8 / 9"));
        assert!(refreshing.contains("Scan window:"));
    }
}
