//! Content-free checkpoints over Codex's existing projection. Native APIs still
//! own item decoding, history repair and lineage. Unknown stores fall back to them.
use std::{os::unix::fs::MetadataExt, path::PathBuf, process::Stdio};

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{fs, io::AsyncReadExt, process::Command, time::timeout};

#[derive(Debug)]
pub(super) struct SourceChanged;
impl std::fmt::Display for SourceChanged {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Native source prefix changed")
    }
}
impl std::error::Error for SourceChanged {}

#[derive(Clone, Deserialize, Serialize)]
pub(super) struct Prefix {
    bytes: u64,
    digest: String,
}

impl Prefix {
    /// A tail-page hash cannot certify earlier history. Check the entire retained
    /// prefix locally; only native IDs of changed items cross the worker boundary.
    pub(super) async fn read(path: PathBuf, bytes: u64, previous: Option<&Self>) -> Result<Self> {
        let mut file = fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&path)
            .await?;
        let before = file.metadata().await?;
        ensure!(
            before.is_file() && before.len() >= bytes,
            "Native source is incomplete"
        );
        if let Some(previous) = previous {
            ensure!(bytes >= previous.bytes, SourceChanged);
        }
        let mut hash = Sha256::new();
        let mut offset = 0;
        let mut buffer = vec![0; 1024 * 1024];
        while offset < bytes {
            let boundary = previous
                .filter(|old| offset < old.bytes)
                .map_or(bytes, |old| old.bytes);
            let length = usize::try_from((boundary - offset).min(buffer.len() as u64))?;
            file.read_exact(&mut buffer[..length]).await?;
            hash.update(&buffer[..length]);
            offset += length as u64;
            if let Some(previous) = previous.filter(|old| old.bytes == offset) {
                ensure!(
                    hex::encode(hash.clone().finalize()) == previous.digest,
                    SourceChanged
                );
            }
        }
        let after = fs::metadata(&path).await?;
        ensure!(
            after.dev() == before.dev() && after.ino() == before.ino() && after.len() >= bytes,
            "Native source changed during validation"
        );
        Ok(Self {
            bytes,
            digest: hex::encode(hash.finalize()),
        })
    }
    pub(super) async fn verify(&self, path: PathBuf) -> Result<()> {
        Self::read(path, self.bytes, Some(self)).await?;
        Ok(())
    }
}

#[derive(Clone, PartialEq, Eq, Deserialize, Serialize)]
pub(super) struct StoreIdentity {
    path: PathBuf,
    device: String,
    inode: String,
    schema: String,
}

#[derive(Clone, Deserialize, Serialize)]
pub(super) struct Checkpoint {
    pub revision: u32,
    pub store: StoreIdentity,
    pub prefix: Prefix,
    pub after: u64,
    pub through: u64,
    pub position: Option<Position>,
    pub tail: bool,
    pub refresh: bool,
}

#[derive(Clone, Deserialize, Serialize)]
pub(super) struct Position {
    pub ordinal: u64,
    pub item_id: String,
}

#[derive(Deserialize)]
pub(super) struct Row {
    pub item_id: String,
    pub turn_id: String,
    pub ordinal: u64,
    pub order: u64,
}

#[derive(Deserialize)]
pub(super) struct Page {
    pub identity: StoreIdentity,
    pub bytes: u64,
    pub maximum: u64,
    pub rows: Vec<Row>,
}

#[derive(Serialize)]
struct Request<'a> {
    home: PathBuf,
    thread: &'a str,
    after: Option<u64>,
    through: Option<u64>,
    position: Option<&'a Position>,
}

pub(super) async fn page(
    home: PathBuf,
    thread: &str,
    checkpoint: Option<&Checkpoint>,
) -> Result<Page> {
    let request = Request {
        home,
        thread,
        after: checkpoint.map(|value| value.after),
        through: checkpoint.map(|value| value.through),
        position: checkpoint.and_then(|value| value.position.as_ref()),
    };
    // Node and its native SQLite binding already ship with every worker. Opening
    // read-only avoids a second Harness, migrations, journal changes or a mirror.
    let output = timeout(
        std::time::Duration::from_secs(30),
        Command::new("node")
            .args(["--no-warnings", "--input-type=module", "--eval", READER])
            .arg(serde_json::to_string(&request)?)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .output(),
    )
    .await??;
    ensure!(
        output.status.success() && output.stdout.len() < 64 * 1024,
        "Native projection is unavailable"
    );
    let page: Page =
        serde_json::from_slice(&output.stdout).context("Invalid native projection metadata")?;
    ensure!(
        page.rows.len() <= 26
            && page
                .rows
                .iter()
                .all(|row| !row.item_id.is_empty() && !row.turn_id.is_empty()),
        "Invalid native projection page"
    );
    if let Some(checkpoint) = checkpoint {
        ensure!(
            page.identity == checkpoint.store && page.maximum >= checkpoint.through,
            "Native projection identity changed"
        );
    }
    Ok(page)
}

const READER: &str = r#"
import {DatabaseSync} from 'node:sqlite';
import {createHash} from 'node:crypto';
import {lstatSync,realpathSync} from 'node:fs';
import {join} from 'node:path';
const input=JSON.parse(process.argv[1]), path=join(input.home,'thread_history_1.sqlite');
const stat=lstatSync(path,{bigint:true});
if(!stat.isFile()||stat.isSymbolicLink()||realpathSync(path)!==path)throw Error('store identity');
const db=new DatabaseSync(path,{readOnly:true});
try{
 db.exec('PRAGMA query_only=ON; BEGIN');
 for(const [table,columns] of Object.entries({
  thread_items:['thread_id','turn_id','item_id','rollout_ordinal','updated_at_ordinal'],
  thread_history_projection_state:['thread_id','next_rollout_byte_offset','next_rollout_ordinal']})){
  const actual=new Set(db.prepare(`PRAGMA table_info(${table})`).all().map(row=>row.name));
  if(!columns.every(column=>actual.has(column)))throw Error('unknown schema');
 }
 const migrations=db.prepare('SELECT version,hex(checksum) AS checksum FROM _sqlx_migrations WHERE success=1 ORDER BY version').all();
 if(migrations.length===0)throw Error('unknown schema');
 const schema=createHash('sha256').update(JSON.stringify(migrations)).digest('hex');
 const state=db.prepare('SELECT next_rollout_byte_offset AS bytes,next_rollout_ordinal AS ordinal FROM thread_history_projection_state WHERE thread_id=?').get(input.thread);
 if(!state||!Number.isSafeInteger(state.bytes)||state.bytes<0||!Number.isSafeInteger(state.ordinal))throw Error('unknown projection');
 const maximum=db.prepare('SELECT coalesce(max(updated_at_ordinal),0) AS maximum FROM thread_items WHERE thread_id=?').get(input.thread).maximum;
 let rows=[];
 if(input.after!==null&&input.after<input.through){
  const position=input.position??{ordinal:input.after,item_id:''};
  if(![input.after,input.through,position.ordinal].every(value=>Number.isSafeInteger(value)&&value>=0))throw Error('invalid checkpoint');
  const lower=Math.max(input.after+1,position.ordinal);
  rows=db.prepare(`SELECT i.item_id,i.turn_id,i.updated_at_ordinal AS ordinal,
   (SELECT count(*) FROM thread_items p WHERE p.thread_id=i.thread_id AND p.rollout_ordinal<i.rollout_ordinal) AS 'order'
   FROM thread_items i WHERE i.thread_id=? AND i.updated_at_ordinal>=?
   AND i.updated_at_ordinal<=? AND (i.updated_at_ordinal>? OR (i.updated_at_ordinal=? AND i.item_id>?))
   ORDER BY i.updated_at_ordinal,i.item_id LIMIT 26`).all(input.thread,lower,input.through,position.ordinal,position.ordinal,position.item_id);
 }
 if(!Number.isSafeInteger(maximum)||maximum<0||state.ordinal<=maximum||rows.some(row=>typeof row.item_id!=='string'||typeof row.turn_id!=='string'||row.item_id.length>512||row.turn_id.length>512||!Number.isSafeInteger(row.ordinal)||!Number.isSafeInteger(row.order)||row.ordinal<0||row.order<0))throw Error('invalid ordinal');
 process.stdout.write(JSON.stringify({identity:{path,device:String(stat.dev),inode:String(stat.ino),schema},bytes:state.bytes,maximum,rows}));
 db.exec('ROLLBACK');
}finally{db.close()}
"#;
