//! GitHub notification intake through the existing Switchboard GitHub CLI.
//! Includes read threads in a seven-day initial window, then overlaps settled
//! windows by two minutes. GitHub's notification feed is not a repository archive.
//! Each native CLI request reads one bounded page of a frozen time window.

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    protocol::SourceRef,
    sources::{
        ReadOutput, Source, SourceBatch, SourceCheckpoint, SourceConfig, SourceCoverage,
        SourceEvent, SourceKey, now_seconds, read_command,
    },
};

#[derive(Debug)]
pub struct GitHubSource {
    namespace: String,
}

#[derive(Deserialize)]
struct Registry {
    namespaces: Vec<Namespace>,
}
#[derive(Deserialize)]
struct Namespace {
    id: String,
    provider: String,
}
#[derive(Debug, Deserialize, Serialize)]
struct Window {
    #[serde(default)]
    version: u8,
    account: String,
    since: u64,
    until: u64,
    not_before: u64,
    #[serde(default = "first_page")]
    page: u32,
    #[serde(default = "default_poll_interval")]
    poll_interval: u64,
}
#[derive(Deserialize, Serialize)]
struct Envelope {
    status: String,
    fields: Option<Fields>,
}
#[derive(Deserialize, Serialize)]
struct Fields {
    status: String,
    stdout_text: String,
}
#[derive(Deserialize)]
struct Notification {
    #[serde(default)]
    id: String,
    #[serde(default)]
    updated_at: String,
    #[serde(default)]
    reason: String,
    repository: Option<Repository>,
    subject: Option<Subject>,
}
#[derive(Deserialize)]
struct Repository {
    full_name: String,
}
#[derive(Deserialize)]
struct Subject {
    title: String,
    #[serde(rename = "type")]
    kind: String,
    url: Option<String>,
    latest_comment_url: Option<String>,
}
struct Page {
    notifications: Vec<Notification>,
    next_page: Option<u32>,
    poll_interval: u64,
}
struct Scan {
    notifications: Vec<Notification>,
    next_page: Option<u32>,
    complete_page: bool,
    pages: u32,
    poll_interval: u64,
}

fn first_page() -> u32 {
    1
}
fn default_poll_interval() -> u64 {
    60
}

impl GitHubSource {
    pub fn new(namespace: String) -> Result<Self> {
        ensure!(
            !namespace.trim().is_empty(),
            "GitHub source needs an explicit namespace"
        );
        Ok(Self { namespace })
    }

    pub async fn discover(selected: Option<&[String]>) -> Result<Vec<Self>> {
        let output = read_command(&[
            "switchboard".into(),
            "ns".into(),
            "list".into(),
            "--json".into(),
        ])
        .await?;
        output.check_discovery()?;
        Self::from_registry(&output.stdout, selected)
    }

    fn from_registry(output: &str, selected: Option<&[String]>) -> Result<Vec<Self>> {
        let registry: Registry =
            serde_json::from_str(output).context("Invalid Switchboard namespace registry")?;
        let namespaces: Vec<_> = registry
            .namespaces
            .into_iter()
            .filter(|item| item.provider == "github")
            .collect();
        if let Some(selected) = selected {
            for namespace in selected {
                ensure!(
                    namespaces.iter().any(|item| &item.id == namespace),
                    "Requested GitHub namespace is not configured"
                );
            }
        }
        namespaces
            .into_iter()
            .filter(|item| selected.is_none_or(|ids| ids.contains(&item.id)))
            .map(|item| Self::new(item.id))
            .collect()
    }

    fn window(&self, checkpoint: Option<&SourceCheckpoint>, now: u64) -> Result<Window> {
        let mut window = match checkpoint {
            Some(checkpoint) => serde_json::from_str(&checkpoint.value)
                .context("Invalid GitHub source checkpoint")?,
            None => Window {
                version: 1,
                account: self.namespace.clone(),
                since: now.saturating_sub(7 * 86_400),
                until: 0,
                not_before: 0,
                page: 1,
                poll_interval: 60,
            },
        };
        ensure!(
            window.account == self.namespace,
            "GitHub checkpoint belongs to another account"
        );
        ensure!(window.version <= 1, "Unsupported GitHub checkpoint version");
        // Old aggregate scans never admitted an intermediate native page. Start
        // those checkpoints at the committed baseline rather than reusing their
        // obsolete page field. Current continuations retain the frozen cutoff.
        if window.version == 0 || window.until == 0 {
            window.until = now.saturating_add(1);
            window.page = 1;
            window.poll_interval = 60;
        }
        window.version = 1;
        ensure!(
            window.page > 0 && window.page <= 1_000_000,
            "GitHub checkpoint page is invalid"
        );
        ensure!(
            window.since < window.until,
            "GitHub checkpoint window is invalid"
        );
        Ok(window)
    }

    fn normalize(&self, output: ReadOutput, window: Window, now: u64) -> SourceBatch {
        output.normalize(|output| self.normalize_output(output, window, now))
    }

    fn normalize_output(&self, output: ReadOutput, mut window: Window, now: u64) -> SourceBatch {
        let key = self.key();
        let Ok(envelope) = serde_json::from_str::<Envelope>(&output.stdout) else {
            return SourceBatch::unavailable(
                key,
                "GitHub helper returned no valid result; check scoped provider access",
            );
        };
        let Some(fields) = envelope.fields else {
            return SourceBatch::unavailable(key, "GitHub result has no notification coverage");
        };
        let Ok(scan) = parse_scan(&fields.stdout_text) else {
            return SourceBatch::unavailable(
                key,
                "GitHub notifications could not be read completely; checkpoint retained",
            );
        };
        let mut invalid = false;
        let events = scan
            .notifications
            .into_iter()
            .filter_map(|notification| {
                let (Some(repository), Some(subject)) =
                    (notification.repository, notification.subject)
                else {
                    invalid = true;
                    return None;
                };
                if parse_timestamp(&notification.updated_at).is_err() {
                    invalid = true;
                    return None;
                }
                if notification.id.is_empty()
                    || repository.full_name.is_empty()
                    || subject.title.is_empty()
                {
                    invalid = true;
                    return None;
                }
                let mut text = format!(
                    "Repository: {}\nType: {}\nReason: {}\nUpdated: {}\nSubject: {}",
                    repository.full_name,
                    subject.kind,
                    notification.reason,
                    notification.updated_at,
                    subject.title
                );
                if let Some(url) = subject.url {
                    text.push_str(&format!("\nSubject API: {url}"));
                }
                if let Some(url) = subject.latest_comment_url {
                    text.push_str(&format!("\nLatest comment API: {url}"));
                }
                let title = format!("{}: {}", repository.full_name, subject.title);
                // Updated time is provenance, never a pagination key.
                let mut digest = Sha256::new();
                for part in [&title, &text] {
                    digest.update((part.len() as u64).to_be_bytes());
                    digest.update(part.as_bytes());
                }
                let version = format!(
                    "{}:{}",
                    notification.updated_at,
                    hex::encode(digest.finalize())
                );
                Some(SourceEvent {
                    occurred_at: None,
                    id: notification.id.clone(),
                    version,
                    legacy_version: None,
                    source: SourceRef {
                        provider: key.provider.clone(),
                        account: key.account.clone(),
                        thread_id: notification.id,
                    },
                    title,
                    text,
                })
            })
            .collect();
        let mut batch = SourceBatch {
            progress: None,
            source: key,
            events,
            checkpoint: None,
            coverage: SourceCoverage::Partial {
                message: "GitHub notification coverage is incomplete; checkpoint retained".into(),
            },
        };
        if output.exit_code != 0
            || envelope.status != "executed"
            || fields.status != "ok"
            || invalid
            || output.truncated
            || !scan.complete_page
        {
            return batch;
        }
        window.poll_interval = window.poll_interval.max(scan.poll_interval);
        if let Some(next) = scan.next_page {
            if window.page.checked_add(scan.pages) != Some(next) {
                return batch;
            }
            window.page = next;
            window.not_before = 0;
            batch.coverage = SourceCoverage::More;
        } else {
            window.not_before = now.saturating_add(window.poll_interval.max(60));
            window.since = window.until.saturating_sub(120);
            window.until = 0;
            window.page = 1;
            batch.coverage = SourceCoverage::Complete;
        }
        if let Ok(value) = serde_json::to_string(&window) {
            batch.checkpoint = Some(SourceCheckpoint { value });
        }
        batch
    }
}

#[async_trait::async_trait]
impl Source for GitHubSource {
    fn configuration(&self) -> SourceConfig {
        SourceConfig::GitHub {
            enabled: true,
            namespaces: Some(vec![self.namespace.clone()]),
        }
    }
    fn key(&self) -> SourceKey {
        SourceKey {
            provider: "github".into(),
            account: self.namespace.clone(),
        }
    }

    async fn poll(&self, checkpoint: Option<&SourceCheckpoint>) -> Result<SourceBatch> {
        let now = now_seconds()?;
        let window = self.window(checkpoint, now)?;
        if now < window.not_before {
            let settled: Window = serde_json::from_str(
                &checkpoint
                    .context("GitHub poll interval requires a checkpoint")?
                    .value,
            )?;
            return Ok(SourceBatch {
                progress: None,
                source: self.key(),
                events: Vec::new(),
                checkpoint: checkpoint.cloned(),
                coverage: if settled.until == 0 {
                    SourceCoverage::Complete
                } else {
                    SourceCoverage::More
                },
            });
        }
        // Keep headers for pagination/polling limits, and project native JSON
        // before capture so repository metadata cannot crowd out notifications.
        let argv = vec![
            "switchboard".into(), "github.cli.read".into(), "--ns".into(), self.namespace.clone(), "--json".into(), "--".into(),
            "api".into(), "notifications".into(), "--method".into(), "GET".into(), "--include".into(),
            "-F".into(), "all=true".into(), "-F".into(), "per_page=50".into(),
            "-F".into(), format!("since={}", utc_timestamp(window.since)?),
            "-F".into(), format!("before={}", utc_timestamp(window.until)?),
            "-F".into(), format!("page={}", window.page),
            "--jq".into(), "map({id,updated_at,reason,repository:{full_name:.repository.full_name},subject:{title:.subject.title,url:.subject.url,latest_comment_url:.subject.latest_comment_url,type:.subject.type}})".into(),
        ];
        Ok(match read_command(&argv).await {
            Ok(output) => self.normalize(output, window, now_seconds()?),
            Err(_) => SourceBatch::unavailable(
                self.key(),
                "GitHub helper could not run; check scoped read-only authorization",
            ),
        })
    }
}

/// A page contains native HTTP headers followed by its projected JSON array.
/// Accept retained aggregate fixtures too; a malformed later page keeps earlier
/// evidence but cannot advance its durable checkpoint.
fn parse_scan(output: &str) -> Result<Scan> {
    let mut scan = Scan {
        notifications: Vec::new(),
        next_page: None,
        complete_page: false,
        pages: 0,
        poll_interval: 60,
    };
    let mut remaining = output.trim_start();
    let mut parsed_page = false;
    loop {
        let (page, tail) = match parse_page(remaining) {
            Ok(page) => page,
            Err(_) if parsed_page => return Ok(scan),
            Err(error) => return Err(error),
        };
        parsed_page = true;
        scan.pages += 1;
        scan.notifications.extend(page.notifications);
        scan.poll_interval = scan.poll_interval.max(page.poll_interval);
        remaining = tail.trim_start();
        if remaining.is_empty() {
            scan.next_page = page.next_page;
            scan.complete_page = true;
            return Ok(scan);
        }
        if page.next_page.is_none() {
            return Ok(scan);
        }
    }
}

fn parse_page(output: &str) -> Result<(Page, &str)> {
    let (headers, body) = output
        .split_once("\r\n\r\n")
        .or_else(|| output.split_once("\n\n"))
        .context("Missing native GitHub response headers")?;
    let status = headers
        .lines()
        .next()
        .context("Missing GitHub HTTP status")?;
    ensure!(
        status.starts_with("HTTP/") && status.split_whitespace().nth(1) == Some("200"),
        "GitHub notification request failed"
    );
    let mut next_page = None;
    let mut poll_interval = 60;
    for line in headers.lines().skip(1) {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if name.eq_ignore_ascii_case("x-poll-interval") {
            poll_interval = value
                .trim()
                .parse()
                .context("Invalid GitHub poll interval")?;
        }
        if name.eq_ignore_ascii_case("link") {
            for link in value.split(',') {
                let (target, params) = link
                    .split_once('>')
                    .context("Invalid GitHub pagination link")?;
                ensure!(
                    target.trim_start().starts_with('<'),
                    "Invalid GitHub pagination target"
                );
                let relation = params
                    .split(';')
                    .find_map(|param| param.trim().strip_prefix("rel="))
                    .context("Missing GitHub pagination relation")?;
                let is_next = relation
                    .trim_matches('"')
                    .split_whitespace()
                    .any(|relation| relation == "next");
                if is_next {
                    ensure!(next_page.is_none(), "Duplicate GitHub next-page relation");
                    let target = target
                        .trim()
                        .strip_prefix('<')
                        .context("Invalid GitHub pagination target")?;
                    let (endpoint, query) = target
                        .split_once('?')
                        .context("Missing GitHub pagination query")?;
                    ensure!(
                        endpoint.starts_with("https://") && endpoint.ends_with("/notifications"),
                        "GitHub pagination target changed endpoint"
                    );
                    let page = query
                        .split('&')
                        .find_map(|part| part.strip_prefix("page="))
                        .context("Missing GitHub next-page number")?
                        .parse::<u32>()?;
                    ensure!(
                        page > 1 && page <= 1_000_000,
                        "Invalid GitHub next-page number"
                    );
                    next_page = Some(page);
                }
            }
        }
    }
    let mut json = serde_json::Deserializer::from_str(body).into_iter::<Vec<Notification>>();
    let notifications = json.next().context("Missing GitHub notification array")??;
    Ok((
        Page {
            notifications,
            next_page,
            poll_interval,
        },
        &body[json.byte_offset()..],
    ))
}

fn utc_timestamp(seconds: u64) -> Result<String> {
    let date = chrono::DateTime::from_timestamp(seconds.try_into()?, 0)
        .context("GitHub checkpoint date is invalid")?;
    Ok(date.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
}

fn parse_timestamp(value: &str) -> Result<u64> {
    Ok(chrono::DateTime::parse_from_rfc3339(value)?
        .timestamp()
        .try_into()?)
}

#[cfg(test)]
mod tests {
    use crate::sources::{
        ReadOutput, Source, SourceCoverage,
        github::{Envelope, Fields, GitHubSource, Window, utc_timestamp},
    };

    const NOTIFICATION: &str = r#"[{"id":"123456","updated_at":"2023-11-14T21:00:00Z","reason":"review_requested","repository":{"full_name":"example/board"},"subject":{"title":"Verify board change","type":"PullRequest","url":"https://api.github.com/repos/example/board/pulls/7","latest_comment_url":null}}]"#;
    fn source() -> GitHubSource {
        GitHubSource::new("github.example".into()).unwrap()
    }
    fn output(body: &str, next: bool) -> ReadOutput {
        let link = if next {
            "Link: <https://api.github.com/notifications?page=2>; rel=\"next\", <https://api.github.com/notifications?page=4>; rel=\"last\"\r\n"
        } else {
            ""
        };
        ReadOutput {
            exit_code: 0,
            truncated: false,
            stdout: serde_json::to_string(&Envelope {
                status: "executed".into(),
                fields: Some(Fields {
                    status: "ok".into(),
                    stdout_text: format!("HTTP/2.0 200 OK\n{link}X-Poll-Interval: 120\r\n\n{body}"),
                }),
            })
            .unwrap(),
        }
    }

    fn paginated(pages: &[ReadOutput]) -> ReadOutput {
        let stdout_text = pages
            .iter()
            .map(|page| {
                serde_json::from_str::<Envelope>(&page.stdout)
                    .unwrap()
                    .fields
                    .unwrap()
                    .stdout_text
            })
            .collect::<Vec<_>>()
            .join("\n");
        ReadOutput {
            exit_code: 0,
            truncated: false,
            stdout: serde_json::to_string(&Envelope {
                status: "executed".into(),
                fields: Some(Fields {
                    status: "ok".into(),
                    stdout_text,
                }),
            })
            .unwrap(),
        }
    }

    #[test]
    fn native_paginated_scan_accepts_nonmonotonic_rows_and_migrates_the_legacy_checkpoint() {
        let source = source();
        let checkpoint = crate::sources::SourceCheckpoint {
            value: r#"{"account":"github.example","since":1699395200,"until":1700000001,"before":1699995601,"page":2,"not_before":0}"#.into(),
        };
        let newer = NOTIFICATION
            .replace("123456", "123457")
            .replace("21:00:00Z", "23:00:00Z");
        let batch = source.normalize(
            paginated(&[output(NOTIFICATION, true), output(&newer, false)]),
            source.window(Some(&checkpoint), 1_710_000_000).unwrap(),
            1_710_000_000,
        );
        assert_eq!(batch.coverage, SourceCoverage::Complete);
        assert_eq!(batch.events.len(), 2);
        assert_eq!(batch.events[1].id, "123457");
        let saved: Window = serde_json::from_str(&batch.checkpoint.unwrap().value).unwrap();
        assert_eq!(saved.since, 1_710_000_001 - 120);
        assert_eq!(saved.until, 0);
        assert_eq!(saved.not_before, 1_710_000_120);
    }

    #[test]
    fn native_paginated_late_failure_retains_the_baseline_without_forging_completion() {
        let source = source();
        // Switchboard returns provider_failed without fields when gh fails on a
        // later page. Previously admitted events remain in the durable journal.
        let scan = ReadOutput {
            exit_code: 1,
            truncated: false,
            stdout: serde_json::to_string(&Envelope {
                status: "error".into(),
                fields: None,
            })
            .unwrap(),
        };
        let batch = source.normalize(
            scan,
            source.window(None, 1_700_000_000).unwrap(),
            1_700_000_000,
        );
        assert!(batch.events.is_empty());
        assert!(matches!(batch.coverage, SourceCoverage::Unavailable { .. }));
        assert!(batch.checkpoint.is_none());
    }

    #[test]
    fn helper_storage_failure_explains_unavailable_notification_coverage() {
        let source = source();
        let batch = source.normalize(
            ReadOutput {
                exit_code: 1,
                truncated: false,
                stdout: crate::sources::tests::STORAGE_FAILURE.into(),
            },
            source.window(None, 1_700_000_000).unwrap(),
            1_700_000_000,
        );
        assert!(batch.events.is_empty() && batch.checkpoint.is_none());
        assert!(
            matches!(batch.coverage, SourceCoverage::Unavailable { message } if message.starts_with("Source helper storage failure during persistence;") && !message.contains("private-token"))
        );
    }

    #[test]
    fn native_cli_notification_preserves_identity_version_and_scan_start_cutoff() {
        let source = source();
        let batch = source.normalize(
            output(NOTIFICATION, false),
            source.window(None, 1_700_000_000).unwrap(),
            1_700_000_000,
        );
        assert_eq!(batch.coverage, SourceCoverage::Complete);
        let event = &batch.events[0];
        assert_eq!(event.id, "123456");
        assert_eq!(event.source.thread_id, "123456");
        assert!(event.version.starts_with("2023-11-14T21:00:00Z:"));
        assert_eq!(event.source.provider, "github");
        assert_eq!(event.source.account, "github.example");
        assert!(event.text.contains("review_requested") && event.text.contains("/pulls/7"));
        let next = source
            .window(batch.checkpoint.as_ref(), 1_710_000_000)
            .unwrap();
        assert_eq!(next.since, 1_700_000_001 - 120);
        assert_eq!(next.until, 1_710_000_001);
        assert_eq!(next.not_before, 1_700_000_120);
        assert!(
            GitHubSource::new("github.other".into())
                .unwrap()
                .window(batch.checkpoint.as_ref(), 1_710_000_000)
                .is_err()
        );
    }

    #[test]
    fn changed_projected_content_gets_a_distinct_version_without_moving_the_cursor() {
        let source = source();
        let normalize = |body: &str| {
            source.normalize(
                output(body, false),
                source.window(None, 1_700_000_000).unwrap(),
                1_700_000_000,
            )
        };
        let original = normalize(NOTIFICATION);
        let replay = normalize(NOTIFICATION);
        let read = normalize(&NOTIFICATION.replace(
            "\"reason\":",
            "\"unread\":false,\"last_read_at\":\"2023-11-14T21:01:00Z\",\"reason\":",
        ));
        assert_eq!(
            serde_json::to_string(&original.events).unwrap(),
            serde_json::to_string(&replay.events).unwrap()
        );
        assert_eq!(
            serde_json::to_string(&original.events).unwrap(),
            serde_json::to_string(&read.events).unwrap()
        );
        for changed in [
            NOTIFICATION.replace("Verify board change", "Review board change"),
            NOTIFICATION.replace("example/board", "example/renamed-board"),
            NOTIFICATION.replace("/pulls/7", "/pulls/8"),
        ] {
            let changed = normalize(&changed);
            assert_ne!(original.events[0].text, changed.events[0].text);
            assert_ne!(original.events[0].version, changed.events[0].version);
            assert_eq!(original.events[0].id, changed.events[0].id);
            assert_eq!(
                serde_json::to_string(&original.events[0].source).unwrap(),
                serde_json::to_string(&changed.events[0].source).unwrap()
            );
            assert_eq!(original.checkpoint, changed.checkpoint);
        }
    }

    #[test]
    fn empty_array_is_complete_and_next_window_keeps_overlap_from_the_frozen_cutoff() {
        let source = source();
        let batch = source.normalize(
            output("[]", false),
            source.window(None, 1_700_000_000).unwrap(),
            1_710_000_000,
        );
        assert_eq!(batch.coverage, SourceCoverage::Complete);
        assert!(batch.events.is_empty());
        let saved: Window = serde_json::from_str(&batch.checkpoint.unwrap().value).unwrap();
        assert_eq!(saved.since, 1_700_000_001 - 120);
        assert_eq!(saved.until, 0);
        assert_eq!(
            utc_timestamp(1_700_000_000).unwrap(),
            "2023-11-14T22:13:20Z"
        );
    }

    #[test]
    fn each_valid_page_advances_its_continuation_before_the_scan_finishes() {
        let source = source();
        let missing = source.normalize(
            output(NOTIFICATION, true),
            source.window(None, 1_700_000_000).unwrap(),
            1_700_000_000,
        );
        assert_eq!(missing.coverage, SourceCoverage::More);
        assert!(missing.checkpoint.is_some());
        assert_eq!(missing.events[0].id, "123456");
        let complete = source.normalize(
            paginated(&[output(NOTIFICATION, true), output("[]", false)]),
            source.window(None, 1_700_000_000).unwrap(),
            1_700_000_000,
        );
        assert_eq!(complete.coverage, SourceCoverage::Complete);
        assert_eq!(complete.events.len(), 1);
        assert!(complete.checkpoint.is_some());
    }

    #[derive(serde::Deserialize)]
    struct PageCheckpoint {
        since: u64,
        until: u64,
        page: u32,
        not_before: u64,
    }

    #[test]
    fn page_checkpoint_continues_the_frozen_window_after_restart() {
        let source = source();
        let first = source.normalize(
            output(NOTIFICATION, true),
            source.window(None, 1_700_000_000).unwrap(),
            1_700_000_000,
        );
        assert_eq!(
            first.coverage,
            SourceCoverage::More,
            "A complete native page must make durable progress without reading all remaining pages"
        );
        let checkpoint = first.checkpoint.unwrap();
        let saved: PageCheckpoint = serde_json::from_str(&checkpoint.value).unwrap();
        assert_eq!(saved.page, 2);
        assert_eq!(saved.until, 1_700_000_001);
        assert_eq!(saved.not_before, 0);
        let reopened = source.window(Some(&checkpoint), 1_700_000_500).unwrap();
        let retained: PageCheckpoint =
            serde_json::from_str(&serde_json::to_string(&reopened).unwrap()).unwrap();
        assert_eq!(retained.until, saved.until);
        assert_eq!(retained.since, saved.since);
        assert_eq!(retained.page, 2);
        let second = source.normalize(output("[]", false), reopened, 1_700_000_500);
        assert_eq!(second.coverage, SourceCoverage::Complete);
        let completed: PageCheckpoint =
            serde_json::from_str(&second.checkpoint.unwrap().value).unwrap();
        assert_eq!(completed.since, saved.until - 120);
        assert_eq!(completed.until, 0);
        assert_eq!(completed.not_before, 1_700_000_620);
    }

    #[test]
    fn an_unexpected_next_page_cannot_skip_unread_notification_pages() {
        let source = source();
        let mut response = output(NOTIFICATION, true);
        response.stdout = response
            .stdout
            .replace("notifications?page=2", "notifications?page=3");
        let page = source.normalize(
            response,
            source.window(None, 1_700_000_000).unwrap(),
            1_700_000_000,
        );
        assert!(matches!(page.coverage, SourceCoverage::Partial { .. }));
        assert!(page.checkpoint.is_none());
        assert_eq!(page.events.len(), 1);
    }

    #[test]
    fn failed_partial_and_truncated_reads_never_advance() {
        let source = source();
        let mut partial = output(NOTIFICATION, true);
        partial.exit_code = 1;
        let batch = source.normalize(
            partial,
            source.window(None, 1_700_000_000).unwrap(),
            1_700_000_000,
        );
        assert_eq!(batch.events.len(), 1);
        assert!(matches!(batch.coverage, SourceCoverage::Partial { .. }));
        assert!(batch.checkpoint.is_none());
        let mut truncated = output(NOTIFICATION, true);
        truncated.truncated = true;
        let batch = source.normalize(
            truncated,
            source.window(None, 1_700_000_000).unwrap(),
            1_700_000_000,
        );
        assert!(matches!(batch.coverage, SourceCoverage::Partial { .. }));
        assert_eq!(batch.events.len(), 1);
        assert!(batch.checkpoint.is_none());
        let mut denied = output("[]", false);
        denied.stdout = denied.stdout.replace("200 OK", "403 Forbidden");
        let batch = source.normalize(
            denied,
            source.window(None, 1_700_000_000).unwrap(),
            1_700_000_000,
        );
        assert!(matches!(batch.coverage, SourceCoverage::Unavailable { .. }));
        assert!(batch.checkpoint.is_none());
        let batch = source.normalize(
            output(&NOTIFICATION.replace("\"123456\"", "\"\""), false),
            source.window(None, 1_700_000_000).unwrap(),
            1_700_000_000,
        );
        assert!(matches!(batch.coverage, SourceCoverage::Partial { .. }));
        assert!(batch.checkpoint.is_none());
    }

    #[test]
    fn namespace_discovery_is_optional_and_rejects_other_provider_accounts() {
        let registry = r#"{"namespaces":[{"id":"github.one","provider":"github"},{"id":"github.two","provider":"github"},{"id":"google.example","provider":"google"}]}"#;
        assert_eq!(
            GitHubSource::from_registry(registry, None).unwrap().len(),
            2
        );
        let selected = GitHubSource::from_registry(registry, Some(&["github.two".into()])).unwrap();
        assert_eq!(selected[0].key().account, "github.two");
        assert!(GitHubSource::from_registry(registry, Some(&["google.example".into()])).is_err());
        assert!(
            GitHubSource::from_registry(r#"{"namespaces":[]}"#, None)
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn server_poll_interval_defers_the_native_read_without_forging_new_progress() {
        let source = source();
        let mut window = source
            .window(None, crate::sources::now_seconds().unwrap())
            .unwrap();
        window.not_before = u64::MAX;
        let checkpoint = crate::sources::SourceCheckpoint {
            value: serde_json::to_string(&window).unwrap(),
        };
        let batch = source.poll(Some(&checkpoint)).await.unwrap();
        assert!(batch.events.is_empty());
        assert_eq!(batch.coverage, SourceCoverage::More);
        assert_eq!(batch.checkpoint.as_ref(), Some(&checkpoint));
    }

    #[tokio::test]
    #[ignore = "requires an existing scoped Switchboard GitHub namespace with notification read access"]
    async fn live_notification_read_uses_the_existing_github_cli_namespace() {
        let namespace = std::env::var("AGENT_TEST_GITHUB_NAMESPACE")
            .expect("Set AGENT_TEST_GITHUB_NAMESPACE to an existing namespace");
        let source = GitHubSource::discover(Some(&[namespace]))
            .await
            .unwrap()
            .pop()
            .unwrap();
        let batch = source.poll(None).await.unwrap();
        assert_eq!(batch.coverage, SourceCoverage::Complete);
        assert!(batch.checkpoint.is_some());
        eprintln!(
            "Native GitHub CLI read verified: {} notifications normalized with durable checkpoint candidate.",
            batch.events.len()
        );
    }
}
