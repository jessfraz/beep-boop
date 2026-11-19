//! Bounded read-only source pages. Pi owns timing, admission and checkpoints.
use crate::{
    background::{SourcePollCursor, SourceRead},
    protocol::SourceState,
    sources::{SourceBatch, SourceConfig, SourceCoverage},
};
use anyhow::{Context, Result, ensure};

/// Discovery validates the configured scopes once. Returned descriptors contain
/// exactly one discovered account, so page reads do not rediscover the catalog.
pub async fn discover(
    config: &[SourceConfig],
    states: &[SourceState],
) -> Result<(Vec<SourceConfig>, Vec<SourceRead>)> {
    let catalog = crate::sources::configured(config).await;
    let descriptors = catalog
        .sources
        .into_iter()
        .map(|source| source.configuration())
        .collect();
    let reads = catalog
        .failures
        .into_iter()
        .map(|batch| {
            let expected_checkpoint = states
                .iter()
                .find(|state| state.source == batch.source)
                .and_then(|state| state.checkpoint.clone());
            SourceRead {
                batch,
                expected_checkpoint,
            }
        })
        .collect();
    Ok((descriptors, reads))
}

pub async fn poll_source(config: &SourceConfig, state: Option<&SourceState>) -> Result<SourceRead> {
    use crate::sources::{
        Source, calendar::CalendarSource, github::GitHubSource, google::GoogleSource,
        messages::MessagesSource, slack::SlackSource,
    };
    ensure!(config.enabled(), "Disabled source cannot be collected");
    let account = |namespaces: &Option<Vec<String>>| -> Result<String> {
        let selected = namespaces
            .as_ref()
            .context("A page read needs a discovered account")?;
        let [namespace] = selected.as_slice() else {
            anyhow::bail!("A page read owns exactly one account");
        };
        ensure!(
            !namespace.trim().is_empty(),
            "A page read needs a nonempty account"
        );
        Ok(namespace.clone())
    };
    let source: Box<dyn Source> = match config {
        SourceConfig::Google { namespaces, .. } => {
            Box::new(GoogleSource::new(account(namespaces)?)?)
        }
        SourceConfig::GitHub { namespaces, .. } => {
            Box::new(GitHubSource::new(account(namespaces)?)?)
        }
        SourceConfig::GoogleCalendar { namespaces, .. } => {
            Box::new(CalendarSource::new(account(namespaces)?)?)
        }
        SourceConfig::Messages { namespaces, .. } => {
            if namespaces.as_ref().is_some_and(Vec::is_empty) {
                Box::new(MessagesSource::local())
            } else {
                Box::new(MessagesSource::new(account(namespaces)?)?)
            }
        }
        SourceConfig::Slack {
            command, account, ..
        } => Box::new(SlackSource::new(command.clone(), account.clone())?),
    };
    let key = source.key();
    ensure!(
        state.is_none_or(|state| state.source == key),
        "Source checkpoint belongs to another account"
    );
    let expected_checkpoint = state.and_then(|state| state.checkpoint.clone());
    let mut batch = source
        .poll(expected_checkpoint.as_ref())
        .await
        .unwrap_or_else(|_| {
            SourceBatch::unavailable(key, "Source read failed; coverage is unknown")
        });
    bound_page(&mut batch)?;
    Ok(SourceRead {
        batch,
        expected_checkpoint,
    })
}

fn bound_page(batch: &mut SourceBatch) -> Result<()> {
    if serde_json::to_vec(batch)?.len() > crate::runtime::MAX_FRAME_BYTES / 2 {
        batch.events.clear();
        batch.checkpoint = None;
        batch.coverage = SourceCoverage::Partial {
            message: "Source page exceeds the transport limit; checkpoint retained".into(),
        };
    }
    Ok(())
}

pub async fn poll(
    config: &[SourceConfig],
    states: &[SourceState],
    cursor: Option<SourcePollCursor>,
) -> Result<(Vec<SourceRead>, Option<SourcePollCursor>)> {
    let catalog = crate::sources::configured(config).await;
    let mut reads = Vec::new();
    if cursor.is_none() {
        for batch in catalog.failures {
            let expected_checkpoint = states
                .iter()
                .find(|state| state.source == batch.source)
                .and_then(|state| state.checkpoint.clone());
            reads.push(SourceRead {
                batch,
                expected_checkpoint,
            });
        }
    }
    let mut sources = catalog.sources.into_iter();
    let (source, page) = if let Some(cursor) = cursor {
        ensure!(cursor.page < 4, "Source continuation page is invalid");
        let source = sources
            .find(|source| source.key() == cursor.source)
            .context("Source accounts changed during collection; saved checkpoints are retained")?;
        (Some(source), cursor.page)
    } else {
        (sources.next(), 0)
    };
    let Some(source) = source else {
        return Ok((reads, None));
    };
    let key = source.key();
    let expected_checkpoint = states
        .iter()
        .find(|state| state.source == key)
        .and_then(|state| state.checkpoint.clone());
    let mut batch = source
        .poll(expected_checkpoint.as_ref())
        .await
        .unwrap_or_else(|_| SourceBatch {
            progress: None,
            source: key.clone(),
            events: Vec::new(),
            checkpoint: None,
            coverage: SourceCoverage::Unavailable {
                message: "Source read failed; coverage is unknown".into(),
            },
        });
    // Each page is admitted before another is read. Keep room for protocol
    // framing even when provider normalization expands an unusually large page.
    bound_page(&mut batch)?;
    let next = if batch.coverage == SourceCoverage::More && page < 3 {
        Some(SourcePollCursor {
            source: key,
            page: page + 1,
        })
    } else {
        sources.next().map(|source| SourcePollCursor {
            source: source.key(),
            page: 0,
        })
    };
    reads.push(SourceRead {
        batch,
        expected_checkpoint,
    });
    Ok((reads, next))
}
