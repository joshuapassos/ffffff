use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize, de::value};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::PathBuf;
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

    fn _to_bytes(&self) -> [u8; Header::SIZE] {
        let mut bytes = [0u8; Header::SIZE];
        bytes[0..8].copy_from_slice(&self.total_size.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.keys.to_le_bytes());
        bytes[16..24].copy_from_slice(&self.lookup_start.to_le_bytes());
        bytes[24..32].copy_from_slice(&self.start_data.to_le_bytes());
        bytes[32..40].copy_from_slice(&self.offset_free.to_le_bytes());
        bytes
    }

    fn _from_bytes(bytes: &[u8]) -> Self {
        let total_size = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let keys = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        let lookup_start = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
        let start_data = u64::from_le_bytes(bytes[24..32].try_into().unwrap());
        let offset_free = u64::from_le_bytes(bytes[32..40].try_into().unwrap());
        Header {
            total_size,
            keys,
            lookup_start,
            start_data,
            offset_free,
        }
    }
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
    const SIZE: usize = 32 + 8 + 1024 + 8 + 8 + 1;

    fn _to_bytes(&self) -> [u8; HashEntry::SIZE] {
        let mut bytes = [0u8; HashEntry::SIZE];
        bytes[0..32].copy_from_slice(&self.hash_key);
        bytes[32..40].copy_from_slice(&self.size_key.to_le_bytes());
        bytes[40..1064].copy_from_slice(&self.key);
        bytes[1064..1072].copy_from_slice(&self.data_offset.to_le_bytes());
        bytes[1072..1080].copy_from_slice(&self.size.to_le_bytes());
        bytes[1080] = self.state;
        bytes
    }

    fn _from_bytes(bytes: &[u8]) -> Self {
        let hash_key = bytes[0..32].try_into().unwrap();
        let size_key = u64::from_le_bytes(bytes[32..40].try_into().unwrap());
        let key = bytes[40..1064].try_into().unwrap();
        let data_offset = u64::from_le_bytes(bytes[1064..1072].try_into().unwrap());
        let size = u64::from_le_bytes(bytes[1072..1080].try_into().unwrap());
        let state = match bytes[1080] {
            0 => EntryState::Used.to_u8(),
            1 => EntryState::Deleted.to_u8(),
            _ => {
                debug!("Invalid entry state: {}", bytes[1080]);
                EntryState::Deleted.to_u8()
            }
        };
        HashEntry {
            hash_key,
            size_key,
            key,
            data_offset,
            size,
            state,
            _padding: [0u8; 7],
        }
    }

    fn is_used(&self) -> bool {
        matches!(EntryState::from_u8(self.state), EntryState::Used)
    }
}

pub struct Storage {
    path: PathBuf,
    header: Header,
    file: std::fs::File,
    mmap: memmap2::MmapMut,
    // mmap1kb: memmap2::MmapMut,
    // mmap10kb: memmap2::MmapMut,
    // mmap100kb: memmap2::MmapMut,
    btree: Index,
}

impl Storage {
    pub fn open(path: PathBuf, initial_size: u64) -> std::io::Result<Self> {
        std::fs::create_dir_all(".data")?;
        let mut data_path = std::path::PathBuf::from(".data");

        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(data_path.clone().join("data.store"))?;

        let metadata = file.metadata().unwrap();

        if metadata.len() < Header::SIZE as u64 {
            file.set_len(initial_size)?;
        }

        let mut mmap = unsafe { memmap2::MmapMut::map_mut(&file)? };

        let check = Header::read_from_bytes(&mmap[0..Header::SIZE]).unwrap();

        debug!("Storage header: {:?}", check);

        let header = if check.total_size == 0 && check.keys == 0 {
            let header = Header {
                total_size: initial_size,
                keys: 0 as u64,
                lookup_start: Header::SIZE as u64,
                start_data: Header::SIZE as u64 + 1024 * 1024 * 1024 * 4,
                offset_free: Header::SIZE as u64 + 1024 * 1024 * 1024 * 4,
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

        debug!("Loading storage with {} keys", header.keys);

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

        Ok(Storage {
            btree: Index::new(btree_elements),
            path,
            file,
            header,
            mmap,
        })
    }

    pub fn get_header(&self) -> &Header {
        &self.header
    }

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

        self.btree.add(&entry.hash_key, p_0);

        let _ = self.mmap.flush_async();

        Ok(())
    }

    pub fn get_all(&self) -> Vec<([u8; 32], &[u8])> {
        let mut results = Vec::new();
        let mut offset = self.header.lookup_start;

        while offset < self.header.lookup_start + (self.header.keys * HashEntry::SIZE as u64) {
            let entry = HashEntry::read_from_bytes(
                &self.mmap[offset as usize..(offset + HashEntry::SIZE as u64) as usize],
            )
            .unwrap();
            if entry.is_used() {
                let hash_key = entry.hash_key;
                let value = &self.mmap
                    [entry.data_offset as usize..(entry.data_offset + entry.size) as usize];
                results.push((hash_key, value));
            }
            offset += HashEntry::SIZE as u64;
        }

        results
    }

    pub fn get_all_keys(&self) -> Vec<[u8; 32]> {
        let result = self.get_all();
        result.into_iter().map(|(k, _)| k).collect()
    }

    pub fn flush(&mut self) -> std::io::Result<()> {
        self.mmap.flush()?;
        self.file.sync_all()
    }

    pub fn search_p(&self, key: &[u8]) -> Option<Vec<u8>> {
        let mut buff: Vec<u8> = Vec::new();
        let mut offset = self.header.lookup_start;

        while offset < self.header.lookup_start + (self.header.keys * HashEntry::SIZE as u64) {
            let entry = HashEntry::read_from_bytes(
                &self.mmap[offset as usize..(offset + HashEntry::SIZE as u64) as usize],
            )
            .unwrap();
            if &entry.key[0..key.len() as usize] == key && entry.is_used() {
                buff.append(
                    &mut self.mmap
                        [entry.data_offset as usize..(entry.data_offset + entry.size) as usize]
                        .to_vec(),
                );
                buff.append(b"\r".to_vec().as_mut());
            }
            offset += HashEntry::SIZE as u64;
        }

        if buff.len() > 0 { Some(buff) } else { None }
    }

    pub fn get_el(&self, offset: u64) -> &[u8] {
        let entry = HashEntry::read_from_bytes(
            &self.mmap[offset as usize..(offset + HashEntry::SIZE as u64) as usize],
        )
        .unwrap();
        let data_offset = entry.data_offset;
        let data_size = entry.size;
        &self.mmap[data_offset as usize..(data_offset + data_size) as usize]
    }

    pub fn get_by_btree(&self, key: &[u8]) -> Option<&[u8]> {
        let str_key = str::from_utf8(key).unwrap();

        match self.btree.get(str_key) {
            Some(offset) => {
                let entry = HashEntry::read_from_bytes(
                    &self.mmap[*offset as usize..(*offset + HashEntry::SIZE as u64) as usize],
                ).unwrap();
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
        match self.btree.get(k) {
            Some(offset) => {
                let mut entry = HashEntry::read_from_bytes(
                    &self.mmap[*offset as usize..(*offset + HashEntry::SIZE as u64) as usize],
                ).unwrap();
                entry.state = EntryState::Deleted.to_u8();
                self.mmap[*offset as usize..(*offset + HashEntry::SIZE as u64) as usize]
                    .copy_from_slice(&entry.as_mut_bytes());
                self.btree.del(k);
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

        let mut hm = HashMap::with_capacity(10_000_000);

        hm.extend(m.into_iter().map(|(k, v)| (k, v)).into_iter());

        Index {
            index: hm
        }
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

    // // from https://docs.rs/bincode/latest/bincode/#example
    // fn serialize(&self) -> Vec<u8> {
    //     bincode::encode_to_vec(
    //         &self.b_tree_1kb,
    //         bincode::config::standard().with_fixed_int_encoding(),
    //     )
    //     .unwrap()
    // }
    // // from https://docs.rs/bincode/latest/bincode/#example
    // fn deserialize(data: &[u8]) -> Self {
    //     let (b_tree_1kb, _): (BTreeMap<Vec<u8>, u64>, _) =
    //         bincode::decode_from_slice(data, bincode::config::standard().with_fixed_int_encoding())
    //             .unwrap();
    //     BTree { b_tree_1kb }
    // }
}
