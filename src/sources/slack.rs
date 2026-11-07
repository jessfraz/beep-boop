//! Indexed Slack-message intake through a configured read-only helper. Search
//! is not a complete workspace archive or private-thread read. Date-only search
//! keeps yesterday and today, roughly 24–48 hours with boundary overlap.

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    protocol::SourceRef,
    sources::{
        ReadOutput, Source, SourceBatch, SourceCheckpoint, SourceCoverage, SourceEvent, SourceKey,
        SourceScanPhase, SourceScanProgress, SourceScanUnit, now_seconds, read_command,
    },
};

#[derive(Debug)]
pub struct SlackSource {
    command: Vec<String>,
    account: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Window {
    account: String,
    since_day: u64,
    until_day: u64,
    // Legacy page checkpoints replay their frozen window once. Native message
    // identity deduplicates that replay; page offsets cannot become cursors.
    #[serde(default = "first_cursor")]
    cursor: String,
    // Legacy in-flight windows first observe a recent page, then resume the
    // exact saved backfill cursor. Neither read resets the other's coverage.
    #[serde(default = "recent_first")]
    recent_next: bool,
    #[serde(default)]
    messages_read: Option<u64>,
    #[serde(default)]
    total: Option<u64>,
}

fn recent_first() -> bool {
    true
}

impl Window {
    fn progress(&self) -> SourceScanProgress {
        SourceScanProgress {
            window_start: self.since_day.saturating_mul(86_400),
            window_end: self.until_day.saturating_mul(86_400),
            phase: SourceScanPhase::Reading,
            unit: SourceScanUnit::Messages,
            completed: self.messages_read,
            total: self.total,
        }
    }
}

fn first_cursor() -> String {
    "*".into()
}

#[derive(Deserialize)]
struct Response {
    ok: bool,
    messages: Option<Messages>,
}
#[derive(Deserialize)]
struct Messages {
    matches: Vec<Message>,
    paging: Paging,
}
#[derive(Deserialize)]
struct Paging {
    next_cursor: Option<String>,
    total: Option<u64>,
}
#[derive(Deserialize)]
struct Message {
    #[serde(default)]
    ts: String,
    #[serde(default)]
    thread_ts: Option<String>,
    #[serde(default)]
    text: String,
    channel: Channel,
    edited: Option<Edited>,
}
#[derive(Deserialize)]
struct Channel {
    id: String,
    #[serde(default)]
    name: String,
}
#[derive(Deserialize)]
struct Edited {
    ts: String,
}

impl SlackSource {
    pub fn new(command: Vec<String>, account: String) -> Result<Self> {
        ensure!(
            command.first().is_some_and(|program| !program.is_empty()),
            "Slack source needs a read-only helper command"
        );
        ensure!(
            !account.is_empty(),
            "Slack source needs an explicit account"
        );
        Ok(Self { command, account })
    }

    fn window(&self, checkpoint: Option<&SourceCheckpoint>, now: u64) -> Result<Window> {
        let day = now / 86_400;
        let mut window = match checkpoint {
            Some(checkpoint) => serde_json::from_str(&checkpoint.value)
                .context("Invalid Slack source checkpoint")?,
            None => Window {
                account: self.account.clone(),
                since_day: day.saturating_sub(1),
                until_day: 0,
                cursor: first_cursor(),
                recent_next: true,
                messages_read: Some(0),
                total: None,
            },
        };
        ensure!(
            window.account == self.account,
            "Slack checkpoint belongs to another account"
        );
        if window.until_day == 0 {
            window.until_day = day.saturating_add(1);
        }
        // Retain a two-day scan while it intersects the current lookback, so
        // midnight cannot discard unread messages still inside that horizon.
        // Old wider scans retire only through successful checkpoint admission;
        // journal originals, decisions and tasks are never removed here.
        if window.until_day <= day.saturating_sub(1)
            || window.until_day.saturating_sub(window.since_day) > 2
        {
            window = Window {
                account: self.account.clone(),
                since_day: day.saturating_sub(1),
                until_day: day.saturating_add(1),
                cursor: first_cursor(),
                recent_next: true,
                messages_read: Some(0),
                total: None,
            };
        }
        ensure!(
            window.since_day < window.until_day
                && !window.cursor.is_empty()
                && !window.cursor.chars().any(char::is_control),
            "Slack checkpoint window is invalid"
        );
        Ok(window)
    }

    fn normalize(&self, output: ReadOutput, window: Window) -> SourceBatch {
        output.normalize(|output| self.normalize_output(output, window))
    }

    fn normalize_output(&self, output: ReadOutput, mut window: Window) -> SourceBatch {
        let key = self.key();
        if output.truncated {
            return SourceBatch::unavailable(
                key,
                "Slack helper output was truncated; checkpoint retained",
            );
        }
        let Ok(response) = serde_json::from_str::<Response>(&output.stdout) else {
            return SourceBatch::unavailable(
                key,
                "Slack helper returned no valid result; check read-only authorization",
            );
        };
        let Some(messages) = response.messages else {
            return SourceBatch::unavailable(
                key,
                "Slack returned no message coverage; check read-only authorization",
            );
        };
        let mut invalid = messages.matches.len() > 100;
        let count = messages.matches.len() as u64;
        let events = messages
            .matches
            .into_iter()
            .filter_map(|message| {
                if message.ts.is_empty() || message.channel.id.is_empty() {
                    invalid = true;
                    return None;
                }
                let id = format!("{}:{}", message.channel.id, message.ts);
                let thread_id = format!(
                    "{}:{}",
                    message.channel.id,
                    message.thread_ts.as_deref().unwrap_or(&message.ts)
                );
                let channel = if message.channel.name.is_empty() {
                    &message.channel.id
                } else {
                    &message.channel.name
                };
                let title = format!("Slack: {channel}");
                let occurred_at = message_millis(&message.ts);
                let legacy_version = message.edited.map_or(message.ts, |edited| edited.ts);
                // Search can return edited text without an edited timestamp.
                // Version the normalized immutable content, excluding cursors
                // and other polling metadata.
                let mut digest = Sha256::new();
                for part in [&thread_id, &title, &message.text] {
                    digest.update((part.len() as u64).to_be_bytes());
                    digest.update(part.as_bytes());
                }
                let version = hex::encode(digest.finalize());
                Some(SourceEvent {
                    occurred_at,
                    id,
                    version,
                    legacy_version: Some(legacy_version),
                    source: SourceRef {
                        provider: key.provider.clone(),
                        account: key.account.clone(),
                        thread_id,
                    },
                    title,
                    text: message.text,
                })
            })
            .collect();
        let mut batch = SourceBatch {
            progress: None,
            source: key,
            events,
            checkpoint: None,
            coverage: SourceCoverage::Partial {
                message: "Slack search coverage is incomplete; checkpoint retained".into(),
            },
        };
        if !response.ok || output.exit_code != 0 || invalid {
            return batch;
        }
        let Some(next) = messages.paging.next_cursor else {
            return batch;
        };
        if next == window.cursor || next == "*" || next.chars().any(char::is_control) {
            return batch;
        }
        window.messages_read = if window.cursor == "*" {
            Some(count)
        } else {
            window
                .messages_read
                .and_then(|read| read.checked_add(count))
        };
        window.total = messages.paging.total.or(window.total);
        batch.progress = Some(window.progress());
        window.recent_next = true;
        if !next.is_empty() {
            window.cursor = next;
            batch.coverage = SourceCoverage::More;
        } else {
            // The next scan retains a calendar-day overlap. Old windows are
            // expired only by window(), after account validation.
            window.since_day = window.until_day.saturating_sub(2);
            window.until_day = 0;
            window.cursor = first_cursor();
            window.messages_read = Some(0);
            window.total = None;
            batch.coverage = SourceCoverage::Complete;
        }
        if let Ok(value) = serde_json::to_string(&window) {
            batch.checkpoint = Some(SourceCheckpoint { value });
        }
        batch
    }

    async fn read_page(&self, window: Window, descending: bool) -> Result<SourceBatch> {
        // Slack permits 100 matches/request. A large page gets one bounded
        // retry at the prior 25-match size, using the exact same native cursor.
        // No extra source continuation or provider-side mutation is introduced.
        for count in [100, 25] {
            let mut args = self.command.clone();
            args.extend([
                "search.messages".into(),
                format!(
                    "query=after:{} before:{}",
                    utc_date(window.since_day)?,
                    utc_date(window.until_day)?
                ),
                "sort=timestamp".into(),
                format!("sort_dir={}", if descending { "desc" } else { "asc" }),
                format!("count={count}"),
                format!("cursor={}", window.cursor),
            ]);
            let output = match read_command(&args).await {
                Ok(output) => output,
                Err(_) => {
                    return Ok(SourceBatch::unavailable(
                        self.key(),
                        "Slack helper could not run; check read-only authorization",
                    ));
                }
            };
            let truncated = output.truncated;
            let batch = self.normalize(output, window.clone());
            let oversized = serde_json::to_vec(&batch)?.len() > crate::runtime::MAX_FRAME_BYTES / 2;
            if count == 100 && (truncated || oversized) {
                continue;
            }
            if oversized {
                return Ok(SourceBatch::unavailable(
                    self.key(),
                    "Slack page exceeds the transport limit; checkpoint retained",
                ));
            }
            return Ok(batch);
        }
        Ok(SourceBatch::unavailable(
            self.key(),
            "Slack page is unavailable; checkpoint retained",
        ))
    }
}

#[async_trait::async_trait]
impl Source for SlackSource {
    fn key(&self) -> SourceKey {
        SourceKey {
            provider: "slack".into(),
            account: self.account.clone(),
        }
    }

    async fn poll(&self, checkpoint: Option<&SourceCheckpoint>) -> Result<SourceBatch> {
        let now = now_seconds()?;
        let mut window = self.window(checkpoint, now)?;
        if !window.recent_next {
            return self.read_page(window, false).await;
        }
        // This newest-page observation is deliberately not another archive
        // cursor. Bursts beyond its limit are still covered by backfill.
        let mut recent = self.read_page(self.window(None, now)?, true).await?;
        recent.progress = Some(window.progress());
        if matches!(
            recent.coverage,
            SourceCoverage::Complete | SourceCoverage::More
        ) {
            window.recent_next = false;
            recent.coverage = SourceCoverage::More;
            recent.checkpoint = Some(SourceCheckpoint {
                value: serde_json::to_string(&window)?,
            });
        }
        Ok(recent)
    }
}

fn message_millis(ts: &str) -> Option<u64> {
    let (seconds, fraction) = ts.split_once('.')?;
    if seconds.is_empty()
        || !seconds.bytes().all(|byte| byte.is_ascii_digit())
        || !(1..=6).contains(&fraction.len())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    let micros = fraction
        .parse::<u32>()
        .ok()?
        .checked_mul(10_u32.pow(6 - fraction.len() as u32))?;
    seconds
        .parse::<u64>()
        .ok()?
        .checked_mul(1000)?
        .checked_add(u64::from(micros / 1000))
        .filter(|millis| *millis <= 9_007_199_254_740_991)
}

fn utc_date(day: u64) -> Result<String> {
    let seconds: libc::time_t = day
        .checked_mul(86_400)
        .context("Slack checkpoint date is too large")?
        .try_into()?;
    let mut date: libc::tm = unsafe { std::mem::zeroed() };
    // gmtime_r writes only this owned tm and does not use a shared date buffer.
    ensure!(
        !unsafe { libc::gmtime_r(&seconds, &mut date) }.is_null(),
        "Slack checkpoint date is invalid"
    );
    Ok(format!(
        "{:04}-{:02}-{:02}",
        date.tm_year + 1900,
        date.tm_mon + 1,
        date.tm_mday
    ))
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use crate::sources::{
        ReadOutput, Source, SourceCheckpoint, SourceCoverage,
        slack::{SlackSource, message_millis, utc_date},
    };

    const RESPONSE: &str = r#"{"ok":true,"messages":{"matches":[{"ts":"1700000000.000001","thread_ts":"1699999999.000002","text":"Review the board","channel":{"id":"CEXAMPLE","name":"engineering"}}],"paging":{"count":25,"total":3700,"next_cursor":"native-next"},"pagination":{"total_count":3700,"per_page":25,"next_cursor":"native-next","first":0,"last":0}}}"#;
    #[derive(Deserialize, Serialize)]
    struct LegacyCheckpoint {
        account: String,
        since_day: u64,
        until_day: u64,
        page: u32,
    }
    #[derive(Deserialize)]
    struct CursorCheckpoint {
        account: String,
        since_day: u64,
        until_day: u64,
        cursor: String,
    }
    fn source() -> SlackSource {
        SlackSource::new(vec!["slack-read".into()], "workspace-example".into()).unwrap()
    }
    fn output(text: &str) -> ReadOutput {
        ReadOutput {
            exit_code: 0,
            stdout: text.into(),
            truncated: false,
        }
    }

    #[test]
    fn native_search_preserves_identity_and_frozen_cursor_window() {
        let source = source();
        let batch = source.normalize(
            output(RESPONSE),
            source.window(None, 1_700_000_000).unwrap(),
        );
        assert_eq!(batch.coverage, SourceCoverage::More);
        assert_eq!(batch.events[0].id, "CEXAMPLE:1700000000.000001");
        assert_eq!(batch.events[0].occurred_at, Some(1_700_000_000_000));
        let progress = batch.progress.as_ref().unwrap();
        assert_eq!(progress.completed, Some(1));
        assert_eq!(progress.total, Some(3700));
        assert_eq!(
            batch.events[0].source.thread_id,
            "CEXAMPLE:1699999999.000002"
        );
        let next = source
            .window(batch.checkpoint.as_ref(), 1_700_000_100)
            .unwrap();
        let saved: CursorCheckpoint =
            serde_json::from_str(&batch.checkpoint.as_ref().unwrap().value).unwrap();
        assert_eq!(saved.cursor, "native-next");
        assert_eq!(next.until_day, 1_700_000_000 / 86_400 + 1);
        assert!(
            SlackSource::new(vec!["slack-read".into()], "other-example".into())
                .unwrap()
                .window(batch.checkpoint.as_ref(), 1_700_000_000)
                .is_err()
        );
    }

    #[test]
    fn text_edits_without_edit_metadata_receive_distinct_stable_versions() {
        let source = source();
        let original = source.normalize(
            output(RESPONSE),
            source.window(None, 1_700_000_000).unwrap(),
        );
        let changed = RESPONSE.replace("Review the board", "Review the board.");
        let updated = source.normalize(
            output(&changed),
            source.window(None, 1_700_000_000).unwrap(),
        );
        assert_eq!(original.events[0].id, updated.events[0].id);
        assert_eq!(
            original.events[0].source.thread_id,
            updated.events[0].source.thread_id
        );
        assert_ne!(
            original.events[0].version, updated.events[0].version,
            "Search text can change without an edited timestamp"
        );
        let repeated = source.normalize(
            output(&changed),
            source.window(None, 1_700_000_000).unwrap(),
        );
        assert_eq!(updated.events[0].version, repeated.events[0].version);
    }

    #[test]
    fn content_versions_cover_normalized_fields_and_keep_exact_legacy_identity() {
        let source = source();
        let original = source.normalize(
            output(RESPONSE),
            source.window(None, 1_700_000_000).unwrap(),
        );
        assert_eq!(
            original.events[0].legacy_version.as_deref(),
            Some("1700000000.000001")
        );
        let later_poll = source.normalize(
            output(RESPONSE),
            source.window(None, 1_710_000_000).unwrap(),
        );
        assert_eq!(original.events[0].version, later_poll.events[0].version);
        for changed in [
            RESPONSE.replace("engineering", "another-channel-name"),
            RESPONSE.replace("1699999999.000002", "1699999999.000003"),
        ] {
            let updated = source.normalize(
                output(&changed),
                source.window(None, 1_700_000_000).unwrap(),
            );
            assert_ne!(original.events[0].version, updated.events[0].version);
        }
        let response = RESPONSE.replace(
            "\"text\":",
            "\"edited\":{\"ts\":\"1700000001.000001\"},\"text\":",
        );
        let edited = source.normalize(
            output(&response),
            source.window(None, 1_700_000_000).unwrap(),
        );
        assert_eq!(
            edited.events[0].legacy_version.as_deref(),
            Some("1700000001.000001")
        );
        assert_eq!(original.events[0].version, edited.events[0].version);
    }

    #[test]
    fn completion_keeps_overlap_and_failure_does_not_advance() {
        let source = source();
        let complete = RESPONSE.replace("native-next", "");
        let batch = source.normalize(
            output(&complete),
            source.window(None, 1_700_000_000).unwrap(),
        );
        assert_eq!(batch.coverage, SourceCoverage::Complete);
        let next = source
            .window(batch.checkpoint.as_ref(), 1_700_000_100)
            .unwrap();
        assert_eq!(next.since_day, 1_700_000_000 / 86_400 - 1);
        let failed = source.normalize(
            output(&RESPONSE.replace("\"ok\":true", "\"ok\":false")),
            source.window(None, 1_700_000_000).unwrap(),
        );
        assert_eq!(failed.events.len(), 1);
        assert!(matches!(failed.coverage, SourceCoverage::Partial { .. }));
        assert!(failed.checkpoint.is_none());
    }

    #[tokio::test]
    async fn legacy_page_one_hundred_restarts_its_frozen_window_with_native_cursor_pagination() {
        let complete = RESPONSE.replace("native-next", "");
        let day = crate::sources::now_seconds().unwrap() / 86_400;
        let since = day - 1;
        let until = day + 1;
        let query = format!(
            "query=after:{} before:{}",
            utc_date(since).unwrap(),
            utc_date(until).unwrap()
        );
        let script = format!(
            r#"const args = process.argv.slice(2);
if (args.some((arg) => arg.startsWith('page='))) process.exit(21);
const cursor = args.find((arg) => arg.startsWith('cursor='))?.slice(7);
if (args.includes('sort_dir=desc')) {{ process.stdout.write(JSON.stringify({complete})); process.exit(0); }}
if (!args.includes('{query}')) process.exit(22);
if (!args.includes('count=100')) process.exit(24);
if (cursor === '*') process.stdout.write(JSON.stringify({RESPONSE}));
else if (cursor === 'native-next') process.stdout.write(JSON.stringify({complete}));
else process.exit(23);"#
        );
        let source = SlackSource::new(
            vec![
                "node".into(),
                "--input-type=module".into(),
                "-e".into(),
                script,
                "fixture".into(),
            ],
            "workspace-example".into(),
        )
        .unwrap();
        let legacy = SourceCheckpoint {
            value: serde_json::to_string(&LegacyCheckpoint {
                account: "workspace-example".into(),
                since_day: since,
                until_day: until,
                page: 100,
            })
            .unwrap(),
        };
        let recent = source.poll(Some(&legacy)).await.unwrap();
        assert_eq!(
            recent.coverage,
            SourceCoverage::More,
            "A newest-page observation cannot complete backfill"
        );
        let recent_checkpoint = recent.checkpoint.unwrap();
        let saved: CursorCheckpoint = serde_json::from_str(&recent_checkpoint.value).unwrap();
        assert_eq!(
            (saved.since_day, saved.until_day, saved.cursor.as_str()),
            (since, until, "*")
        );
        let first = source.poll(Some(&recent_checkpoint)).await.unwrap();
        assert_eq!(
            first.coverage,
            SourceCoverage::More,
            "A legacy page100 checkpoint must replay its frozen window in cursor mode"
        );
        assert_eq!(first.events.len(), 1);
        let checkpoint = first.checkpoint.unwrap();
        let saved: CursorCheckpoint = serde_json::from_str(&checkpoint.value).unwrap();
        assert_eq!(saved.account, "workspace-example");
        assert_eq!(saved.since_day, since);
        assert_eq!(saved.until_day, until);
        assert_eq!(saved.cursor, "native-next");
        // A new adapter process continues the opaque native checkpoint.
        let reopened = SlackSource::new(source.command.clone(), source.account.clone()).unwrap();
        let recent = reopened.poll(Some(&checkpoint)).await.unwrap();
        let saved: CursorCheckpoint =
            serde_json::from_str(&recent.checkpoint.as_ref().unwrap().value).unwrap();
        assert_eq!(
            (saved.since_day, saved.until_day, saved.cursor.as_str()),
            (since, until, "native-next")
        );
        assert_eq!(recent.coverage, SourceCoverage::More);
        let final_page = reopened.poll(recent.checkpoint.as_ref()).await.unwrap();
        assert_eq!(final_page.coverage, SourceCoverage::Complete);
        let saved: CursorCheckpoint =
            serde_json::from_str(&final_page.checkpoint.unwrap().value).unwrap();
        assert_eq!(saved.since_day, since);
        assert_eq!(saved.until_day, 0);
        assert_eq!(saved.cursor, "*");
    }

    #[test]
    fn native_cursor_failures_preserve_events_without_advancing_the_checkpoint() {
        let source = source();
        for response in [
            RESPONSE.replace("\"ok\":true", "\"ok\":false"),
            RESPONSE.replace("\"next_cursor\":\"native-next\"", "\"next_cursor\":null"),
            RESPONSE.replace("native-next", "*"),
        ] {
            let batch = source.normalize(
                output(&response),
                source.window(None, 1_700_000_000).unwrap(),
            );
            assert_eq!(batch.events.len(), 1);
            assert!(matches!(batch.coverage, SourceCoverage::Partial { .. }));
            assert!(batch.checkpoint.is_none());
        }
        let first = source.normalize(
            output(RESPONSE),
            source.window(None, 1_700_000_000).unwrap(),
        );
        let repeated = source.normalize(
            output(RESPONSE),
            source
                .window(first.checkpoint.as_ref(), 1_700_000_000)
                .unwrap(),
        );
        assert_eq!(repeated.events.len(), 1);
        assert!(matches!(repeated.coverage, SourceCoverage::Partial { .. }));
        assert!(repeated.checkpoint.is_none());
        let mut failed = output(RESPONSE);
        failed.exit_code = 1;
        let batch = source.normalize(failed, source.window(None, 1_700_000_000).unwrap());
        assert_eq!(batch.events.len(), 1);
        assert!(batch.checkpoint.is_none());
        let mut truncated = output(RESPONSE);
        truncated.truncated = true;
        let batch = source.normalize(truncated, source.window(None, 1_700_000_000).unwrap());
        assert!(matches!(batch.coverage, SourceCoverage::Unavailable { .. }));
        assert!(batch.checkpoint.is_none());
    }

    #[test]
    fn unavailable_auth_is_not_an_empty_success() {
        let source = source();
        let batch = source.normalize(
            output(r#"{"ok":false,"error":"token_expired"}"#),
            source.window(None, 1_700_000_000).unwrap(),
        );
        assert!(matches!(batch.coverage, SourceCoverage::Unavailable { .. }));
        assert!(batch.checkpoint.is_none());
    }

    #[test]
    fn helper_storage_failure_does_not_claim_slack_authorization_is_missing() {
        let source = source();
        let batch = source.normalize(
            output(crate::sources::tests::STORAGE_FAILURE),
            source.window(None, 1_700_000_000).unwrap(),
        );
        assert!(batch.events.is_empty() && batch.checkpoint.is_none());
        assert!(
            matches!(batch.coverage, SourceCoverage::Unavailable { message } if message.starts_with("Source helper storage failure during persistence;") && !message.contains("private-token"))
        );
    }

    #[test]
    fn calendar_query_dates_use_utc_without_local_timezone_state() {
        assert_eq!(utc_date(0).unwrap(), "1970-01-01");
        assert_eq!(utc_date(1_700_000_000 / 86_400).unwrap(), "2023-11-14");
        assert_eq!(utc_date(19_782).unwrap(), "2024-02-29");
    }

    #[test]
    fn provider_message_times_are_checked_without_changing_source_identity() {
        assert_eq!(message_millis("1700000000.123456"), Some(1_700_000_000_123));
        assert_eq!(message_millis("1700000000.1"), Some(1_700_000_000_100));
        for invalid in [
            "1700000000",
            "1700000000.",
            "1700000000.1234567",
            "-1.000001",
            "1.1x",
            "18446744073709551615.000001",
        ] {
            assert_eq!(message_millis(invalid), None);
        }
    }

    #[test]
    fn midnight_retains_an_intersecting_backfill_and_expires_only_the_fully_aged_window() {
        let source = source();
        let now = 20_800 * 86_400 + 43_200;
        let mut window = source.window(None, now).unwrap();
        window.cursor = "opaque-backfill".into();
        window.recent_next = false;
        let saved = SourceCheckpoint {
            value: serde_json::to_string(&window).unwrap(),
        };
        let next_day = source.window(Some(&saved), now + 86_400).unwrap();
        assert_eq!(
            next_day.cursor, "opaque-backfill",
            "Unread messages still inside the new lookback must keep their cursor"
        );
        assert_eq!(
            (next_day.since_day, next_day.until_day),
            (window.since_day, window.until_day)
        );
        let expired = source.window(Some(&saved), now + 2 * 86_400).unwrap();
        assert_eq!(expired.cursor, "*");
        assert_eq!((expired.since_day, expired.until_day), (20_801, 20_803));
        assert!(expired.recent_next);
    }

    #[tokio::test]
    async fn failed_recent_reads_keep_the_exact_backfill_cursor_and_do_not_claim_completion() {
        let source = SlackSource::new(
            vec![
                "node".into(),
                "-e".into(),
                "process.stdout.write(JSON.stringify({ok:false,error:'ratelimited'}))".into(),
            ],
            "workspace-example".into(),
        )
        .unwrap();
        let now = crate::sources::now_seconds().unwrap();
        let mut window = source.window(None, now).unwrap();
        window.cursor = "opaque-backfill".into();
        window.messages_read = Some(1000);
        let checkpoint = SourceCheckpoint {
            value: serde_json::to_string(&window).unwrap(),
        };
        let batch = source.poll(Some(&checkpoint)).await.unwrap();
        assert!(matches!(batch.coverage, SourceCoverage::Unavailable { .. }));
        assert!(batch.checkpoint.is_none());
        let preserved = source.window(Some(&checkpoint), now + 100).unwrap();
        assert_eq!(preserved.cursor, "opaque-backfill");
        assert!(preserved.recent_next);
        assert_eq!(preserved.messages_read, Some(1000));
    }

    #[tokio::test]
    async fn oversized_pages_retry_once_at_twenty_five_on_the_same_native_cursor() {
        let directory = tempfile::tempdir().unwrap();
        let complete = RESPONSE.replace("native-next", "");
        let script = format!(
            r#"import {{ writeFileSync }} from 'node:fs';
const args = process.argv.slice(2);
if (!args.includes('cursor=opaque-backfill') || !args.includes('sort_dir=asc')) process.exit(21);
const count = args.find(arg => arg.startsWith('count='))?.slice(6);
writeFileSync(count, '', {{flag:'wx'}});
if (count === '100') process.stdout.write('x'.repeat({}));
else if (count === '25') process.stdout.write(JSON.stringify({complete}));
else process.exit(22);"#,
            crate::runtime::MAX_FRAME_BYTES / 2 + 1
        );
        // The native helper runs from '/', so give its exact owned evidence
        // filenames instead of changing any process-global working directory.
        let script = script.replace(
            "writeFileSync(count,",
            &format!(
                "writeFileSync({} + '/' + count,",
                serde_json::to_string(directory.path()).unwrap()
            ),
        );
        let source = SlackSource::new(
            vec![
                "node".into(),
                "--input-type=module".into(),
                "-e".into(),
                script,
                "fixture".into(),
            ],
            "workspace-example".into(),
        )
        .unwrap();
        let now = crate::sources::now_seconds().unwrap();
        let mut window = source.window(None, now).unwrap();
        window.cursor = "opaque-backfill".into();
        window.recent_next = false;
        let checkpoint = SourceCheckpoint {
            value: serde_json::to_string(&window).unwrap(),
        };
        let batch = source.poll(Some(&checkpoint)).await.unwrap();
        assert_eq!(batch.coverage, SourceCoverage::Complete);
        assert_eq!(batch.events.len(), 1);
        assert!(directory.path().join("100").is_file());
        assert!(directory.path().join("25").is_file());
        assert!(batch.checkpoint.is_some());
    }

    #[tokio::test]
    async fn old_polling_windows_expire_only_after_a_successful_recent_observation() {
        let now = crate::sources::now_seconds().unwrap();
        let day = now / 86_400;
        let complete = RESPONSE.replace("native-next", "");
        let query = format!(
            "query=after:{} before:{}",
            utc_date(day - 1).unwrap(),
            utc_date(day + 1).unwrap()
        );
        let script = format!(
            r#"const args = process.argv.slice(2);
if (!args.includes('{query}') || !args.includes('sort_dir=desc') || !args.includes('cursor=*')) process.exit(21);
process.stdout.write(JSON.stringify({complete}));"#
        );
        let source = SlackSource::new(
            vec!["node".into(), "-e".into(), script, "fixture".into()],
            "workspace-example".into(),
        )
        .unwrap();
        let old = SourceCheckpoint {
            value: serde_json::to_string(&LegacyCheckpoint {
                account: "workspace-example".into(),
                since_day: day - 8,
                until_day: day - 2,
                page: 100,
            })
            .unwrap(),
        };
        let observed = source.poll(Some(&old)).await.unwrap();
        assert_eq!(observed.coverage, SourceCoverage::More);
        assert_eq!(observed.events.len(), 1);
        let candidate = observed.checkpoint.unwrap();
        let current = source.window(Some(&candidate), now).unwrap();
        assert_eq!((current.since_day, current.until_day), (day - 1, day + 1));
        assert_eq!(current.cursor, "*");
        assert!(!current.recent_next);
        // Expiry changes only the candidate checkpoint. A failed native read
        // supplies no checkpoint for the journal to replace the old one with.
        let failed = SlackSource::new(vec!["false".into()], source.account.clone())
            .unwrap()
            .poll(Some(&old))
            .await
            .unwrap();
        assert!(failed.checkpoint.is_none());
        assert!(matches!(
            failed.coverage,
            SourceCoverage::Unavailable { .. }
        ));
        let saved: LegacyCheckpoint = serde_json::from_str(&old.value).unwrap();
        assert_eq!((saved.since_day, saved.until_day), (day - 8, day - 2));
    }
}
