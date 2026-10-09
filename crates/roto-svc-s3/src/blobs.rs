//! Object bodies on the file system.
//!
//! ```text
//! <root>/<bucket>/<escaped key path>                 live (current) objects
//! <root>/.roto/<bucket>/versions/<hash>/<version>    non-current versions
//! <root>/.roto/<bucket>/uploads/<upload>/<part>      in-flight multipart parts
//! <root>/.roto/tmp/                                  staging for atomic writes
//! ```
//! Writes go to a temp file that is renamed into place, so readers never see partial bodies.
//! A key that is also a directory prefix (`a` and `a/b`) is stored as `a/.roto-self`.

use std::fs;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use crate::keypath::{SELF_FILE, key_path};

pub struct Blobs {
    root: PathBuf,
}

/// Where a body ended up, plus files that had to move to make room for a directory.
#[derive(Debug, Default)]
pub struct Placement {
    pub rel: String,
    /// `(old, new)` relative paths of other objects relocated (`a` became `a/.roto-self`).
    pub moved: Vec<(String, String)>,
}

impl Blobs {
    pub fn new(root: PathBuf) -> io::Result<Self> {
        fs::create_dir_all(root.join(".roto").join("tmp"))?;
        Ok(Self { root })
    }

    pub fn abs(&self, rel: &str) -> PathBuf {
        self.root.join(rel)
    }

    fn tmp(&self) -> PathBuf {
        self.root
            .join(".roto")
            .join("tmp")
            .join(uuid::Uuid::new_v4().to_string())
    }

    pub fn create_bucket(&self, bucket: &str) -> io::Result<()> {
        fs::create_dir_all(self.root.join(bucket))?;
        fs::create_dir_all(self.root.join(".roto").join(bucket))
    }

    pub fn remove_bucket(&self, bucket: &str) -> io::Result<()> {
        for dir in [self.root.join(bucket), self.root.join(".roto").join(bucket)] {
            match fs::remove_dir_all(dir) {
                Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(e),
                _ => {}
            }
        }
        Ok(())
    }

    /// Stores `data` as the live body of `bucket`/`key`.
    pub fn write_live(&self, bucket: &str, key: &str, data: &[u8]) -> io::Result<Placement> {
        let tmp = self.tmp();
        fs::write(&tmp, data)?;
        self.place_live(bucket, key, &tmp)
    }

    /// Moves the file at `from` (relative) to the live location of `bucket`/`key`.
    pub fn relocate_live(&self, from: &str, bucket: &str, key: &str) -> io::Result<Placement> {
        let tmp = self.tmp();
        fs::rename(self.abs(from), &tmp)?;
        self.place_live(bucket, key, &tmp)
    }

    fn place_live(&self, bucket: &str, key: &str, staged: &Path) -> io::Result<Placement> {
        let kp = key_path(key);
        let mut placement = Placement::default();
        let mut dir = self.root.join(bucket);
        let mut rel = PathBuf::from(bucket);
        let last = kp.segments.len() - 1;
        for (i, seg) in kp.segments.iter().enumerate() {
            let is_last = i == last && !kp.folder;
            if is_last {
                break;
            }
            dir.push(seg);
            rel.push(seg);
            // A file where a directory is needed: turn it into `<dir>/.roto-self`.
            if dir.is_file() {
                let old = rel.to_string_lossy().replace('\\', "/");
                let side = self.tmp();
                fs::rename(&dir, &side)?;
                fs::create_dir(&dir)?;
                fs::rename(&side, dir.join(SELF_FILE))?;
                placement
                    .moved
                    .push((old.clone(), format!("{old}/{SELF_FILE}")));
            }
        }
        if kp.folder {
            fs::create_dir_all(&dir)?;
            let _ = fs::remove_file(staged);
            placement.rel = String::new();
            return Ok(placement);
        }
        let mut target = dir.join(&kp.segments[last]);
        let mut target_rel = rel.join(&kp.segments[last]);
        fs::create_dir_all(&dir)?;
        if target.is_dir() {
            target.push(SELF_FILE);
            target_rel.push(SELF_FILE);
        }
        fs::rename(staged, &target)?;
        placement.rel = target_rel.to_string_lossy().replace('\\', "/");
        Ok(placement)
    }

    /// Path for a non-current version (or delete-marker-free history) of a key.
    pub fn version_rel(bucket: &str, key: &str, version_id: &str) -> String {
        let h = hex::encode(Sha256::digest(key.as_bytes()));
        format!(".roto/{bucket}/versions/{}/{h}/{version_id}", &h[..2])
    }

    pub fn move_rel(&self, from: &str, to: &str) -> io::Result<()> {
        let dest = self.abs(to);
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::rename(self.abs(from), dest)
    }

    pub fn part_rel(bucket: &str, upload_id: &str, part: i32) -> String {
        format!(".roto/{bucket}/uploads/{upload_id}/{part}")
    }

    pub fn write_rel(&self, rel: &str, data: &[u8]) -> io::Result<()> {
        let dest = self.abs(rel);
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent)?;
        }
        let tmp = self.tmp();
        fs::write(&tmp, data)?;
        fs::rename(tmp, dest)
    }

    pub fn read(&self, rel: &str) -> io::Result<Vec<u8>> {
        fs::read(self.abs(rel))
    }

    pub fn read_range(&self, rel: &str, start: u64, len: u64) -> io::Result<Vec<u8>> {
        let mut f = fs::File::open(self.abs(rel))?;
        f.seek(SeekFrom::Start(start))?;
        let mut buf = Vec::with_capacity(len as usize);
        f.take(len).read_to_end(&mut buf)?;
        Ok(buf)
    }

    /// Deletes a file and prunes directories that became empty (never the bucket directory).
    pub fn remove(&self, rel: &str) -> io::Result<()> {
        match fs::remove_file(self.abs(rel)) {
            Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(e),
            _ => {}
        }
        let mut dir = self.abs(rel);
        while dir.pop() {
            let depth = dir
                .strip_prefix(&self.root)
                .map(|p| p.components().count())
                .unwrap_or(0);
            let inside_bucket = depth > 1 && !dir.starts_with(self.root.join(".roto")) || depth > 2;
            if !inside_bucket || fs::remove_dir(&dir).is_err() {
                break;
            }
        }
        Ok(())
    }

    pub fn remove_dir(&self, rel: &str) -> io::Result<()> {
        match fs::remove_dir_all(self.abs(rel)) {
            Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e),
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn blobs() -> (Blobs, PathBuf) {
        let dir = std::env::temp_dir().join(format!("roto-blobs-{}", uuid::Uuid::new_v4()));
        (Blobs::new(dir.clone()).unwrap(), dir)
    }

    #[test]
    fn objects_are_real_files() {
        let (b, dir) = blobs();
        b.create_bucket("photos").unwrap();
        let p = b.write_live("photos", "2024/cat.jpg", b"meow").unwrap();
        assert_eq!(p.rel, "photos/2024/cat.jpg");
        assert_eq!(
            std::fs::read(dir.join("photos/2024/cat.jpg")).unwrap(),
            b"meow"
        );
        assert_eq!(b.read_range(&p.rel, 1, 2).unwrap(), b"eo");
        b.remove(&p.rel).unwrap();
        assert!(
            !dir.join("photos/2024").exists(),
            "empty directories are pruned"
        );
        assert!(dir.join("photos").exists(), "bucket directory stays");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn key_and_prefix_can_coexist() {
        let (b, dir) = blobs();
        b.create_bucket("b").unwrap();
        let a = b.write_live("b", "a", b"file").unwrap();
        assert_eq!(a.rel, "b/a");
        let ab = b.write_live("b", "a/b", b"nested").unwrap();
        assert_eq!(ab.rel, "b/a/b");
        assert_eq!(
            ab.moved,
            vec![("b/a".to_string(), "b/a/.roto-self".to_string())]
        );
        assert_eq!(b.read("b/a/.roto-self").unwrap(), b"file");
        // Writing `a` again while it is a directory lands in .roto-self.
        let again = b.write_live("b", "a", b"v2").unwrap();
        assert_eq!(again.rel, "b/a/.roto-self");
        assert_eq!(b.read("b/a/b").unwrap(), b"nested");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn overwrite_is_atomic_replace() {
        let (b, dir) = blobs();
        b.create_bucket("b").unwrap();
        b.write_live("b", "k", b"one").unwrap();
        b.write_live("b", "k", b"two").unwrap();
        assert_eq!(b.read("b/k").unwrap(), b"two");
        assert_eq!(std::fs::read_dir(dir.join(".roto/tmp")).unwrap().count(), 0);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
