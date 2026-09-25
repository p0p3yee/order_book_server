//! Bounded byte-oriented JSONL tailing. A newline commits a record, including across writes.
use crate::prelude::*;
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
};

pub(super) struct Tail {
    path: PathBuf,
    file: File,
    pending: Vec<u8>,
    skip_fragment: bool,
}
impl Tail {
    pub(super) fn open(path: PathBuf, at_end: bool) -> Result<Self> {
        let mut file = File::open(&path)?;
        let mut skip_fragment = false;
        if at_end && file.metadata()?.len() > 0 {
            file.seek(SeekFrom::End(-1))?;
            let mut byte = [0];
            file.read_exact(&mut byte)?;
            skip_fragment = byte[0] != b'\n';
        }
        Ok(Self { path, file, pending: Vec::new(), skip_fragment })
    }
    pub(super) fn path(&self) -> &Path {
        &self.path
    }
    pub(super) fn read(&mut self, limit: usize) -> Result<Vec<String>> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let open = self.file.metadata()?;
            let current = fs::metadata(&self.path)?;
            if (open.dev(), open.ino()) != (current.dev(), current.ino()) {
                *self = Self::open(self.path.clone(), false)?;
                return Err("upstream file replaced".into());
            }
        }
        if self.file.metadata()?.len() < self.file.stream_position()? {
            self.file.seek(SeekFrom::Start(0))?;
            self.pending.clear();
            self.skip_fragment = false;
            return Err("upstream file truncated".into());
        }
        let available = self.file.metadata()?.len().saturating_sub(self.file.stream_position()?);
        if available == 0 {
            return Ok(Vec::new());
        }
        let mut chunk = vec![0; limit.min(4 * 1024 * 1024).min(available as usize)];
        let n = self.file.read(&mut chunk)?;
        self.pending.extend_from_slice(&chunk[..n]);
        if self.pending.len() > limit {
            self.pending.clear();
            return Err("JSONL record exceeds buffer limit".into());
        }
        let mut start = 0;
        let mut lines = Vec::new();
        for (i, byte) in self.pending.iter().enumerate() {
            if *byte == b'\n' {
                if self.skip_fragment {
                    self.skip_fragment = false;
                } else if i > start {
                    lines.push(String::from_utf8(self.pending[start..i].to_vec()));
                }
                start = i + 1;
            }
        }
        self.pending.drain(..start);
        Ok(lines.into_iter().collect::<std::result::Result<Vec<_>, _>>()?)
    }
    pub(super) fn drained(&mut self) -> Result<bool> {
        Ok(self.file.stream_position()? == self.file.metadata()?.len())
    }
    pub(super) fn has_fragment(&self) -> bool {
        !self.pending.is_empty()
    }
}

// Select the newest date/hour without walking historical files on each discovery tick.
pub(super) fn latest_file(dir: &Path) -> Result<Option<PathBuf>> {
    if !dir.exists() {
        return Ok(None);
    }
    let mut entries = fs::read_dir(dir)?.collect::<io::Result<Vec<_>>>()?;
    entries.sort_by_key(|entry| {
        let name = entry.file_name().to_string_lossy().into_owned();
        (name.parse::<u64>().unwrap_or_default(), name)
    });
    for entry in entries.into_iter().rev() {
        if entry.file_type()?.is_file() {
            return Ok(Some(entry.path()));
        }
        if entry.file_type()?.is_dir() {
            if let Some(path) = latest_file(&entry.path())? {
                return Ok(Some(path));
            }
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn partial_lines_and_truncation() -> Result<()> {
        let path = std::env::temp_dir().join(format!("ws-tail-{}", std::process::id()));
        fs::write(&path, b"one\npar")?;
        let mut tail = Tail::open(path.clone(), false)?;
        assert_eq!(tail.read(1024)?, vec!["one"]);
        use std::io::Write;
        let mut writer = fs::OpenOptions::new().append(true).open(&path)?;
        writer.write_all(b"tial\n")?;
        assert_eq!(tail.read(1024)?, vec!["partial"]);
        fs::write(&path, b"")?;
        assert!(tail.read(1024).is_err());
        fs::remove_file(path)?;
        Ok(())
    }
}

#[cfg(test)]
mod more_tests {
    use super::*;
    #[test]
    fn replaced_file_and_numeric_rotation_discovery() -> Result<()> {
        let root = std::env::temp_dir().join(format!("ws-rotation-{}", std::process::id()));
        fs::create_dir_all(&root)?;
        let nine = root.join("9");
        let ten = root.join("10");
        fs::write(&nine, b"old\n")?;
        let mut tail = Tail::open(nine.clone(), true)?;
        assert!(tail.read(1024)?.is_empty());
        fs::write(&ten, b"new\n")?;
        assert_eq!(latest_file(&root)?, Some(ten.clone()));
        fs::rename(&ten, &nine)?;
        assert!(tail.read(1024).is_err());
        assert_eq!(tail.read(1024)?, vec!["new"]);
        fs::remove_dir_all(root)?;
        Ok(())
    }
    #[test]
    fn startup_partial_record_is_not_replayed() -> Result<()> {
        let path = std::env::temp_dir().join(format!("ws-partial-{}", std::process::id()));
        fs::write(&path, b"old par")?;
        let mut tail = Tail::open(path.clone(), true)?;
        use std::io::Write;
        let mut writer = fs::OpenOptions::new().append(true).open(&path)?;
        writer.write_all(b"tial\nnext\n")?;
        assert_eq!(tail.read(1024)?, vec!["next"]);
        fs::remove_file(path)?;
        Ok(())
    }
    #[test]
    fn invalid_utf8_record_does_not_poison_following_reads() -> Result<()> {
        let path = std::env::temp_dir().join(format!("ws-utf8-{}", std::process::id()));
        fs::write(&path, b"\xff\n")?;
        let mut tail = Tail::open(path.clone(), false)?;
        assert!(tail.read(1024).is_err());
        use std::io::Write;
        let mut writer = fs::OpenOptions::new().append(true).open(&path)?;
        writer.write_all(b"next\n")?;
        assert_eq!(tail.read(1024)?, vec!["next"]);
        fs::remove_file(path)?;
        Ok(())
    }
}
