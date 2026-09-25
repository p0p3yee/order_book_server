//! SQLite transactions bind selected events to the exact source-file cursor.
use super::*;
use rusqlite::{Connection, OptionalExtension, params};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::Path;

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub(super) struct Cursor {
    path: String,
    offset: u64,
    inode: u64,
    anchor_len: usize,
    anchor: u64,
}
#[derive(Clone, Serialize, Deserialize, Default)]
pub(super) struct Checkpoint {
    pub cursor: Option<Cursor>,
    pub height: Option<u64>,
    pub time: Option<i64>,
}
pub(super) struct Reader {
    root: PathBuf,
    file: Option<BufReader<std::fs::File>>,
    pub cursor: Option<Cursor>,
    pending: Vec<u8>,
    files: Vec<PathBuf>,
    discovery: Instant,
}
fn fingerprint(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf29ce484222325, |h, b| (h ^ u64::from(*b)).wrapping_mul(0x100000001b3))
}
fn inode(m: &std::fs::Metadata) -> u64 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        m.ino()
    }
    #[cfg(not(unix))]
    {
        let _ = m;
        0
    }
}
fn paths(root: &Path) -> crate::Result<Vec<PathBuf>> {
    fn visit(path: &Path, out: &mut Vec<PathBuf>) -> crate::Result<()> {
        if !path.exists() {
            return Ok(());
        }
        let mut entries = std::fs::read_dir(path)?.collect::<std::io::Result<Vec<_>>>()?;
        entries.sort_by_key(|e| {
            let n = e.file_name().to_string_lossy().into_owned();
            (n.parse::<u64>().unwrap_or_default(), n)
        });
        for e in entries {
            if out.len() > 10000 {
                return Err(
                    "wallet source directory exceeds 10000 files; archive old output outside the active directory"
                        .into(),
                );
            }
            let ty = e.file_type()?;
            if ty.is_dir() {
                visit(&e.path(), out)?;
            } else if ty.is_file() {
                out.push(e.path());
            }
        }
        Ok(())
    }
    let mut out = Vec::new();
    visit(root, &mut out)?;
    Ok(out)
}
impl Reader {
    pub(super) fn open(root: PathBuf, saved: Option<Cursor>) -> crate::Result<Self> {
        let files = paths(&root)?;
        let mut reader = Self { root, file: None, cursor: None, pending: vec![], files, discovery: Instant::now() };
        if let Some(cursor) = saved {
            if Path::new(&cursor.path).is_absolute()
                || Path::new(&cursor.path).components().any(|c| !matches!(c, std::path::Component::Normal(_)))
            {
                return Err("invalid wallet cursor path".into());
            }
            let mut file = std::fs::File::open(reader.root.join(&cursor.path))?;
            let meta = file.metadata()?;
            if inode(&meta) != cursor.inode
                || meta.len() < cursor.offset
                || cursor.anchor_len > 64
                || cursor.offset < cursor.anchor_len as u64
            {
                return Err("wallet cursor file replaced or truncated".into());
            }
            if cursor.anchor_len > 0 {
                file.seek(SeekFrom::Start(cursor.offset - cursor.anchor_len as u64))?;
                let mut anchor = vec![0; cursor.anchor_len];
                file.read_exact(&mut anchor)?;
                if fingerprint(&anchor) != cursor.anchor {
                    return Err("wallet cursor content changed".into());
                }
            }
            file.seek(SeekFrom::Start(cursor.offset))?;
            reader.cursor = Some(cursor);
            reader.file = Some(BufReader::with_capacity(64 * 1024, file));
        } else if let Some(path) = reader.files.last().cloned() {
            reader.switch(path, true)?;
        }
        Ok(reader)
    }
    fn switch(&mut self, path: PathBuf, end: bool) -> crate::Result<()> {
        let mut file = std::fs::File::open(&path)?;
        let meta = file.metadata()?;
        let mut offset = if end { meta.len() } else { 0 };
        // A startup partial line is skipped, never interpreted as a complete event.
        if end && offset > 0 {
            file.seek(SeekFrom::Start(offset - 1))?;
            let mut b = [0];
            file.read_exact(&mut b)?;
            if b[0] != b'\n' {
                // Locate its start so the full record can be consumed once its newline arrives.
                let start = offset.saturating_sub(INPUT_BYTES as u64);
                file.seek(SeekFrom::Start(start))?;
                let mut bytes = vec![0; (offset - start) as usize];
                file.read_exact(&mut bytes)?;
                offset = bytes.iter().rposition(|b| *b == b'\n').map_or(start, |i| start + i as u64 + 1);
                if start > 0 && offset == start {
                    return Err("startup partial wallet record exceeds limit".into());
                }
            }
        }
        file.seek(SeekFrom::Start(offset))?;
        self.cursor = Some(Cursor {
            path: path.strip_prefix(&self.root)?.to_string_lossy().into_owned(),
            offset,
            inode: inode(&meta),
            anchor_len: 0,
            anchor: 0,
        });
        self.file = Some(BufReader::with_capacity(64 * 1024, file));
        self.pending.clear();
        Ok(())
    }
    /// Non-consuming EOF check after a work-budget boundary. File position alone is
    /// insufficient because BufReader may have prefetched unprocessed records.
    pub(super) fn at_tip(&mut self) -> crate::Result<bool> {
        if self.discovery.elapsed() >= Duration::from_secs(1) {
            self.files = paths(&self.root)?;
            self.discovery = Instant::now();
        }
        let Some(cursor) = &self.cursor else { return Ok(false) };
        let path = self.root.join(&cursor.path);
        let meta = std::fs::metadata(&path)?;
        if inode(&meta) != cursor.inode || meta.len() < cursor.offset + self.pending.len() as u64 {
            return Err("wallet source replaced or truncated".into());
        }
        Ok(self.pending.is_empty() && meta.len() == cursor.offset && self.files.last() == Some(&path))
    }
    pub(super) fn next(&mut self) -> crate::Result<Option<String>> {
        if self.discovery.elapsed() >= Duration::from_secs(1) || self.file.is_none() {
            self.files = paths(&self.root)?;
            self.discovery = Instant::now();
            if self.file.is_none() {
                if let Some(p) = self.files.last().cloned() {
                    self.switch(p, false)?;
                }
            }
        }
        let Some(file) = self.file.as_mut() else { return Ok(None) };
        let cursor = self.cursor.as_mut().ok_or("missing wallet cursor")?;
        let meta = std::fs::metadata(self.root.join(&cursor.path))?;
        if inode(&meta) != cursor.inode || meta.len() < cursor.offset + self.pending.len() as u64 {
            return Err("wallet source replaced or truncated".into());
        }
        let remaining = INPUT_BYTES.saturating_sub(self.pending.len());
        if remaining == 0 {
            return Err("wallet record exceeds input byte limit".into());
        }
        let n = file.take(remaining as u64).read_until(b'\n', &mut self.pending)?;
        if self.pending.last() == Some(&b'\n') {
            cursor.offset += self.pending.len() as u64;
            cursor.anchor_len = self.pending.len().min(64);
            cursor.anchor = fingerprint(&self.pending[self.pending.len() - cursor.anchor_len..]);
            let bytes = std::mem::take(&mut self.pending);
            return Ok(Some(String::from_utf8(bytes)?));
        }
        if n == 0 {
            let current = self.root.join(&cursor.path);
            if let Some(i) = self.files.iter().position(|p| p == &current) {
                if let Some(next) = self.files.get(i + 1).cloned() {
                    if !self.pending.is_empty() {
                        return Err("partial wallet record at file rotation".into());
                    }
                    self.switch(next, false)?;
                    return self.next();
                }
            }
        }
        Ok(None)
    }
}

pub(super) struct Store {
    pub connection: Connection,
}
impl Store {
    pub(super) fn open(path: &Path) -> crate::Result<Self> {
        let connection = Connection::open(path)?;
        connection.busy_timeout(Duration::from_millis(200))?;
        connection.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA wal_autocheckpoint=256;
            CREATE TABLE IF NOT EXISTS events(seq INTEGER PRIMARY KEY, user TEXT NOT NULL, channel TEXT NOT NULL, identity TEXT NOT NULL UNIQUE, data TEXT NOT NULL, height INTEGER NOT NULL, time INTEGER NOT NULL);
            CREATE INDEX IF NOT EXISTS events_user_seq ON events(user,seq);
            CREATE INDEX IF NOT EXISTS events_time ON events(time);
            CREATE INDEX IF NOT EXISTS order_oid ON events(user,json_extract(data,'$.order.oid'),seq DESC) WHERE channel='orderUpdates';
            CREATE INDEX IF NOT EXISTS order_cloid ON events(user,lower(json_extract(data,'$.order.cloid')),seq DESC) WHERE channel='orderUpdates';
            CREATE INDEX IF NOT EXISTS fill_oid ON events(user,json_extract(data,'$.oid'),seq DESC) WHERE channel='userFills';
            CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS gaps(id INTEGER PRIMARY KEY, time INTEGER NOT NULL, reason TEXT NOT NULL);")?;
        Ok(Self { connection })
    }
    pub(super) fn meta<T: serde::de::DeserializeOwned>(&self, key: &str) -> crate::Result<Option<T>> {
        let raw: Option<String> =
            self.connection.query_row("SELECT value FROM metadata WHERE key=?1", [key], |r| r.get(0)).optional()?;
        Ok(raw.map(|s| serde_json::from_str(&s)).transpose()?)
    }
    pub(super) fn recent(&self) -> crate::Result<Vec<Event>> {
        let mut q = self
            .connection
            .prepare("SELECT seq,user,channel,data,identity,height FROM events ORDER BY seq DESC LIMIT ?1")?;
        let rows = q.query_map([MAX_EVENTS as i64], |r| {
            Ok((
                r.get::<_, u64>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, String>(4)?,
                r.get::<_, u64>(5)?,
            ))
        })?;
        let mut events = Vec::new();
        for row in rows {
            let (seq, user, channel, data, key, height) = row?;
            let data: Value = serde_json::from_str(&data)?;
            let bytes = data.to_string().len() + key.len() + user.len() + 128;
            events.push(Event { seq, user, channel, data, key, bytes, height });
        }
        events.reverse();
        Ok(events)
    }
    pub(super) fn commit(
        &mut self,
        events: &[Event],
        checkpoints: &[Checkpoint; 2],
        seq: u64,
        gaps: &[String],
        config: &ServerConfig,
        coverage: i64,
    ) -> crate::Result<()> {
        let tx = self.connection.transaction()?;
        for e in events {
            let data = e.data.to_string();
            let time = e.data["time"].as_i64().or_else(|| e.data["statusTimestamp"].as_i64()).unwrap_or(coverage);
            let inserted=tx.execute("INSERT INTO events(seq,user,channel,identity,data,height,time) VALUES(?1,?2,?3,?4,?5,?6,?7) ON CONFLICT(identity) DO NOTHING",params![e.seq,e.user,e.channel,e.key,data,e.height,time])?;
            if inserted == 0 {
                let old: String = tx.query_row("SELECT data FROM events WHERE identity=?1", [&e.key], |r| r.get(0))?;
                if !orders::same_record(&serde_json::from_str::<Value>(&old)?, &e.data) {
                    return Err("conflicting duplicate in durable wallet history".into());
                }
            }
        }
        for (key, value) in [
            ("checkpoints", serde_json::to_string(checkpoints)?),
            ("seq", seq.to_string()),
            ("coverage", coverage.to_string()),
            ("wallets", serde_json::to_string(&config.wallets)?),
        ] {
            tx.execute(
                "INSERT INTO metadata(key,value) VALUES(?1,?2) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                params![key, value],
            )?;
        }
        let count: u64 = tx
            .query_row("SELECT COALESCE((SELECT value FROM metadata WHERE key='gap_count'),'0')", [], |r| {
                r.get::<_, String>(0)
            })?
            .parse()?;
        tx.execute("INSERT INTO metadata(key,value) VALUES('gap_count',?1) ON CONFLICT(key) DO UPDATE SET value=excluded.value",[(count+gaps.len() as u64).to_string()])?;
        for gap in gaps {
            tx.execute(
                "INSERT INTO gaps(time,reason) VALUES(?1,?2)",
                params![chrono::Utc::now().timestamp_millis(), gap],
            )?;
        }
        let cutoff = chrono::Utc::now().timestamp_millis() - (config.wallet_history_days as i64) * 86400000;
        tx.execute(
            "DELETE FROM events WHERE time<?1 OR seq <= COALESCE((SELECT seq FROM events ORDER BY seq DESC LIMIT 1 OFFSET ?2),-1)",
            params![cutoff, config.wallet_history_events as i64],
        )?;
        tx.execute("DELETE FROM gaps WHERE id NOT IN (SELECT id FROM gaps ORDER BY id DESC LIMIT 1000)", [])?;
        tx.commit()?;
        Ok(())
    }
}
pub(crate) fn history(path: &Path, user: &str, after: u64, limit: usize) -> crate::Result<Value> {
    let conn = Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    conn.busy_timeout(Duration::from_millis(200))?;
    conn.execute_batch("BEGIN")?;
    let mut q =
        conn.prepare("SELECT seq,channel,data,height FROM events WHERE user=?1 AND seq>?2 ORDER BY seq LIMIT ?3")?;
    let rows = q.query_map(params![user, after, limit as i64], |r| {
        Ok((r.get::<_, u64>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?, r.get::<_, u64>(3)?))
    })?;
    let mut events = Vec::new();
    let mut bytes = 0;
    for row in rows {
        let (seq, channel, data, height) = row?;
        bytes += data.len();
        if bytes > 1024 * 1024 {
            break;
        }
        events.push(
            json!({"sequence":seq,"channel":channel,"data":serde_json::from_str::<Value>(&data)?,"height":height}),
        );
    }
    let oldest: Option<u64> = conn.query_row("SELECT MIN(seq) FROM events WHERE user=?1", [user], |r| r.get(0))?;
    let mut q = conn.prepare("SELECT time,reason FROM gaps ORDER BY id DESC LIMIT 100")?;
    let gaps = q
        .query_map([], |r| Ok(json!({"time":r.get::<_,i64>(0)?,"reason":r.get::<_,String>(1)?})))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let next = events.last().and_then(|e| e["sequence"].as_u64()).unwrap_or(after);
    let more: bool =
        conn.query_row("SELECT EXISTS(SELECT 1 FROM events WHERE user=?1 AND seq>?2)", params![user, next], |r| {
            r.get(0)
        })?;
    Ok(
        json!({"hasMore":more,"user":user,"nextSequence":events.last().map(|v|v["sequence"].clone()).unwrap_or(json!(after)),"events":events,"oldestRetainedSequence":oldest,"gaps":gaps,"historyComplete":false}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    fn root(tag: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!("wallet-storage-{tag}-{}", std::process::id()));
        drop(std::fs::remove_dir_all(&p));
        std::fs::create_dir_all(&p).unwrap();
        p
    }
    #[test]
    fn partial_record_restart_rotation_and_truncation() {
        let root = root("reader");
        let path = root.join("0");
        std::fs::write(&path, b"old\npart").unwrap();
        let mut reader = Reader::open(root.clone(), None).unwrap();
        assert!(reader.next().unwrap().is_none());
        let saved = reader.cursor.clone();
        drop(reader);
        let mut f = std::fs::OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(b"ial\n").unwrap();
        let mut reader = Reader::open(root.clone(), saved).unwrap();
        assert_eq!(reader.next().unwrap().unwrap(), "partial\n");
        let checkpoint = reader.cursor.clone();
        std::fs::write(root.join("1"), b"rotated\n").unwrap();
        reader.discovery = Instant::now() - Duration::from_secs(2);
        assert_eq!(reader.next().unwrap().unwrap(), "rotated\n");
        std::fs::write(&path, b"changed\n").unwrap();
        assert!(Reader::open(root.clone(), checkpoint).is_err());
        std::fs::remove_file(root.join("1")).unwrap();
        assert!(reader.next().is_err());
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn tip_check_uses_consumed_cursor_and_detects_rotation_and_truncation() {
        use std::io::Write;
        let root = root("tip");
        let path = root.join("0");
        std::fs::write(&path, b"").unwrap();
        let mut reader = Reader::open(root.clone(), None).unwrap();
        let mut writer = std::fs::OpenOptions::new().append(true).open(&path).unwrap();
        writer.write_all(&vec![b' '; 1024 * 1024]).unwrap();
        writer.write_all(b"{}\nnext\n").unwrap();
        assert!(!reader.at_tip().unwrap());
        assert!(reader.next().unwrap().unwrap().len() > 1024 * 1024);
        assert!(!reader.at_tip().unwrap()); // next record may already be in BufReader
        assert_eq!(reader.next().unwrap().unwrap(), "next\n");
        assert!(reader.at_tip().unwrap()); // no extra next() call required
        writer.write_all(b"partial").unwrap();
        assert!(reader.next().unwrap().is_none());
        assert!(!reader.at_tip().unwrap());
        writer.write_all(b"\n").unwrap();
        assert_eq!(reader.next().unwrap().unwrap(), "partial\n");
        std::fs::write(root.join("1"), b"rotated\n").unwrap();
        reader.discovery = Instant::now() - Duration::from_secs(2);
        assert!(!reader.at_tip().unwrap());
        assert_eq!(reader.next().unwrap().unwrap(), "rotated\n");
        assert!(reader.at_tip().unwrap());
        std::fs::write(root.join("1"), b"").unwrap();
        assert!(reader.at_tip().is_err());
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn transaction_rollback_retention_and_history_pagination() {
        let root = root("store");
        let path = root.join("journal.sqlite");
        let mut db = Store::open(&path).unwrap();
        let now = chrono::Utc::now().timestamp_millis();
        let events: Vec<_> = (1..=4)
            .map(|seq| Event {
                seq,
                user: "u".into(),
                channel: "userFills".into(),
                data: json!({"time":now,"tid":seq}),
                key: seq.to_string(),
                bytes: 100,
                height: seq,
            })
            .collect();
        let config = ServerConfig { wallet_history_events: 3, ..ServerConfig::default() };
        let checkpoints = [Checkpoint { height: Some(4), ..Default::default() }, Checkpoint::default()];
        db.commit(&events, &checkpoints, 4, &["test gap".into()], &config, now).unwrap();
        assert_eq!(db.recent().unwrap().len(), 3);
        assert_eq!(db.meta::<u64>("seq").unwrap(), Some(4));
        let page = history(&path, "u", 0, 2).unwrap();
        assert_eq!(page["nextSequence"], 3);
        assert_eq!(page["oldestRetainedSequence"], 2);
        assert_eq!(history(&path, "u", 3, 2).unwrap()["events"].as_array().unwrap().len(), 1);
        let mut conflict = events[3].clone();
        conflict.seq = 5;
        conflict.data["tid"] = json!(999);
        assert!(db.commit(&[conflict], &checkpoints, 5, &[], &config, now).is_err());
        assert_eq!(db.meta::<u64>("seq").unwrap(), Some(4));
        db.connection.execute_batch("CREATE TRIGGER fail_cursor BEFORE UPDATE ON metadata WHEN NEW.key='checkpoints' BEGIN SELECT RAISE(ABORT,'test failure'); END;").unwrap();
        let mut e = events[3].clone();
        e.seq = 5;
        e.key = "5".into();
        assert!(db.commit(&[e], &checkpoints, 5, &[], &config, now).is_err());
        assert_eq!(db.meta::<u64>("seq").unwrap(), Some(4));
        assert!(history(&path, "u", 4, 10).unwrap()["events"].as_array().unwrap().is_empty());
        drop(db);
        std::fs::remove_dir_all(root).unwrap();
    }
}

/// Indexed reads preserve the most recent observed state and later-fill evidence.
pub(super) fn order_record(path: &Path, user: &str, oid: &Value) -> crate::Result<Option<(u64, Value, u64, u64)>> {
    let conn = Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    conn.busy_timeout(Duration::from_millis(200))?;
    conn.execute_batch("BEGIN")?;
    let sql = if oid.is_u64() {
        "SELECT seq,data,height FROM events WHERE channel='orderUpdates' AND user=?1 AND json_extract(data,'$.order.oid')=?2 ORDER BY seq DESC LIMIT 1"
    } else {
        "SELECT seq,data,height FROM events WHERE channel='orderUpdates' AND user=?1 AND lower(json_extract(data,'$.order.cloid'))=?2 ORDER BY seq DESC LIMIT 1"
    };
    let identity = if let Some(n) = oid.as_u64() {
        rusqlite::types::Value::Integer(n as i64)
    } else {
        rusqlite::types::Value::Text(oid.as_str().unwrap_or("").to_ascii_lowercase())
    };
    let row: Option<(u64, String, u64)> =
        conn.query_row(sql, params![user, identity], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))).optional()?;
    let Some((seq, data, height)) = row else { return Ok(None) };
    let data: Value = serde_json::from_str(&data)?;
    let fill:u64=conn.query_row("SELECT COALESCE(MAX(seq),0) FROM events WHERE channel='userFills' AND user=?1 AND json_extract(data,'$.oid')=?2",params![user,data["order"]["oid"].as_u64().unwrap_or(0)],|r|r.get(0))?;
    Ok(Some((seq, data, fill, height)))
}

#[cfg(test)]
mod order_lookup_tests {
    use super::*;
    #[test]
    fn durable_numeric_and_cloid_lookup_include_subsequent_fill_evidence() {
        let path = std::env::temp_dir().join(format!("wallet-order-index-{}.sqlite", std::process::id()));
        drop(std::fs::remove_file(&path));
        let mut store = Store::open(&path).unwrap();
        let now = chrono::Utc::now().timestamp_millis();
        let cloid = format!("0x{}", "a".repeat(32));
        let events = vec![
            Event {
                seq: 1,
                user: "u".into(),
                channel: "orderUpdates".into(),
                data: json!({"order":{"oid":7,"cloid":cloid},"statusTimestamp":now}),
                key: "order".into(),
                bytes: 1,
                height: 1,
            },
            Event {
                seq: 2,
                user: "u".into(),
                channel: "userFills".into(),
                data: json!({"oid":7,"time":now}),
                key: "fill".into(),
                bytes: 1,
                height: 1,
            },
        ];
        store.commit(&events, &Default::default(), 2, &[], &ServerConfig::default(), now).unwrap();
        for identity in [json!(7), json!(cloid.to_uppercase().replacen("0X", "0x", 1))] {
            let (seq, _, fill, height) = order_record(&path, "u", &identity).unwrap().unwrap();
            assert_eq!((seq, fill, height), (1, 2, 1));
        }
        assert!(order_record(&path, "another-wallet", &json!(7)).unwrap().is_none());
        assert!(order_record(&path, "u", &json!(8)).unwrap().is_none());
        drop(store);
        std::fs::remove_file(path).unwrap();
    }
}
