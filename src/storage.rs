use sha2::{Digest, Sha256};
use std::{collections::HashMap, hash::{DefaultHasher, Hash, Hasher}};
use tokio::sync::RwLock;
use tracing::debug;
use zerocopy::{FromBytes, IntoBytes};
use zerocopy_derive::{FromBytes, Immutable, IntoBytes, KnownLayout};

#[derive(FromBytes, IntoBytes, Immutable, Debug, PartialEq, Eq)]
#[repr(C)]
struct Header {
    total_size: u64,
    keys: u64,

    lookup_start: u64,

    start_data: u64,
    offset_free: u64,
}

impl Header {
    const SIZE: usize = 8 * 5;
}

#[derive(Debug, PartialEq, Eq, KnownLayout, Default, Immutable, IntoBytes)]
#[repr(u8)]
enum EntryState {
    Used = 0,
    #[default]
    Deleted = 1,
}

impl EntryState {
    fn from_u8(value: u8) -> Self {
        match value {
            0 => EntryState::Used,
            1 => EntryState::Deleted,
            _ => {
                debug!("Invalid entry state: {}", value);
                EntryState::Deleted
            }
        }
    }

    fn to_u8(&self) -> u8 {
        match self {
            EntryState::Used => 0,
            EntryState::Deleted => 1,
        }
    }
}

#[derive(KnownLayout, IntoBytes, FromBytes, Debug, PartialEq, Eq)]
#[repr(C)]
struct HashEntry {
    hash_key: [u8; 32],
    size_key: u64,
    key: [u8; 1024],
    data_offset: u64,
    size: u64,
    state: u8,
    _padding: [u8; 7],
}

impl HashEntry {
    const SIZE: usize = 32 + 8 + 1024 + 8 + 8 + 1 + 7;

    fn is_used(&self) -> bool {
        matches!(EntryState::from_u8(self.state), EntryState::Used)
    }
}

pub struct Storage {
    shards: Vec<RwLock<Shard>>,
}

pub struct Shard {
    header: Header,
    file: std::fs::File,
    mmap: memmap2::MmapMut,
    index: Index,
}

impl Storage {
    pub fn open(initial_size: u64, num_shards: usize) -> std::io::Result<Self> {
        std::fs::create_dir_all(".data")?;
        let data_path = std::path::PathBuf::from(".data");

        let mut shards = Vec::with_capacity(num_shards);

        for id in 0..num_shards {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(data_path.clone().join(format!("shard_{}.store", id)))?;

            let metadata = file.metadata().unwrap();

            if metadata.len() < Header::SIZE as u64 {
                file.set_len(initial_size)?;
            }

            let mut mmap = unsafe { memmap2::MmapMut::map_mut(&file)? };

            let check = Header::read_from_bytes(&mmap[0..Header::SIZE]).unwrap();

            debug!("Shard header: {:?}", check);

            let header = if check.total_size == 0 && check.keys == 0 {
                let header = Header {
                    total_size: initial_size,
                    keys: 0 as u64,
                    lookup_start: Header::SIZE as u64,
                    start_data: Header::SIZE as u64 + 1024 * 1024 * 1024 * 2,
                    offset_free: Header::SIZE as u64 + 1024 * 1024 * 1024 * 2,
                };
                let header_bytes = header.as_bytes();
                (&mut mmap[0..Header::SIZE]).copy_from_slice(&header_bytes);
                mmap.flush()?;
                header
            } else {
                check
            };

            let mut btree_elements = Vec::new();
            let mut offset = header.lookup_start;

            debug!("Loading shard with {} keys", header.keys);

            for _ in 0..header.keys {
                let entry = HashEntry::read_from_bytes(
                    &mmap[offset as usize..(offset + HashEntry::SIZE as u64) as usize],
                );

                if let Ok(entry) = entry {
                    if entry.is_used() {
                        let hash_key = entry.hash_key;
                        let value = offset;

                        btree_elements.push((hash_key, value));
                    }
                }
                offset += HashEntry::SIZE as u64;
            }

            shards.push(RwLock::new(Shard {
                index: Index::new(btree_elements),
                file,
                header,
                mmap,
            }));
        }

        Ok(Storage { shards })
    }

    pub fn get_shard(&self, key: &[u8]) -> &RwLock<Shard> {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash_u64 = hasher.finish();

        let mask = (self.shards.len() - 1) as u64;
        &self.shards[(hash_u64 & mask) as usize]
    }

    pub async fn flush(&self) -> std::io::Result<()> {
        for shard in &self.shards {
            shard.write().await.flush()?;
        }
        Ok(())
    }
}

impl Shard {
    pub fn add(&mut self, key: &[u8], data: &[u8]) -> std::io::Result<()> {
        let position = self.header.offset_free;

        if key.len() > 1024 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Key size exceeds maximum length",
            ));
        }

        let mut buf = [0u8; 1024];

        buf[..key.len()].copy_from_slice(&key);

        let mut entry = HashEntry {
            hash_key: Sha256::digest(key).into(),
            size_key: key.len() as u64,
            key: buf,
            data_offset: position,
            size: data.len() as u64,
            state: EntryState::Used.to_u8(),
            _padding: [0u8; 7],
        };

        self.mmap[position as usize..(position + entry.size) as usize].copy_from_slice(data);

        let p_keys =
            |n| self.header.lookup_start + ((self.header.keys + n) * HashEntry::SIZE as u64);

        let p_0 = p_keys(0);

        self.mmap[p_0 as usize..p_keys(1) as usize].copy_from_slice(&entry.as_mut_bytes());

        self.header.offset_free += entry.size;
        self.header.keys += 1;

        let header_bytes = self.header.as_bytes();
        self.mmap[0..Header::SIZE].copy_from_slice(&header_bytes);

        self.index.add(&entry.hash_key, p_0);

        let _ = self.mmap.flush_async();

        Ok(())
    }

    pub fn flush(&mut self) -> std::io::Result<()> {
        self.mmap.flush()?;
        self.file.sync_all()
    }

    pub fn get_by_btree(&self, key: &[u8]) -> Option<&[u8]> {
        let str_key = str::from_utf8(key).unwrap();

        match self.index.get(str_key) {
            Some(offset) => {
                let entry = HashEntry::read_from_bytes(
                    &self.mmap[*offset as usize..(*offset + HashEntry::SIZE as u64) as usize],
                )
                .unwrap();
                let data_offset = entry.data_offset;
                let data_size = entry.size;
                debug!("Found key '{}' - {:?}", str_key, entry);
                Some(&self.mmap[data_offset as usize..(data_offset + data_size) as usize])
            }
            _ => None,
        }
    }

    pub fn del(&mut self, key: &[u8]) -> std::io::Result<()> {
        let k = str::from_utf8(key).unwrap();
        match self.index.get(k) {
            Some(offset) => {
                let mut entry = HashEntry::read_from_bytes(
                    &self.mmap[*offset as usize..(*offset + HashEntry::SIZE as u64) as usize],
                )
                .unwrap();
                entry.state = EntryState::Deleted.to_u8();
                self.mmap[*offset as usize..(*offset + HashEntry::SIZE as u64) as usize]
                    .copy_from_slice(&entry.as_mut_bytes());
                self.index.del(k);
                self.mmap.flush_async()?;
                Ok(())
            }
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Key not found",
            )),
        }
    }
}

struct Index {
    index: HashMap<[u8; 32], u64>,
}

impl Index {
    fn new(m: Vec<([u8; 32], u64)>) -> Self {
        let mut hm = HashMap::with_capacity(10_000);

        hm.extend(m.into_iter().map(|(k, v)| (k, v)).into_iter());

        Index { index: hm }
    }

    fn add(&mut self, key: &[u8; 32], value: u64) {
        self.index.insert(*key, value);
    }

    fn get(&self, key: &str) -> Option<&u64> {
        let mut buf = [0; 32];
        let f = Sha256::digest(key);
        buf.copy_from_slice(&f);

        self.index.get(&buf)
    }

    fn del(&mut self, key: &str) {
        let mut buf = [0; 32];
        let f = Sha256::digest(key);
        buf.copy_from_slice(&f);

        self.index.remove(&buf);
    }
}
