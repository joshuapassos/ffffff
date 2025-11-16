use std::path::PathBuf;
use std::collections::BTreeMap;

#[derive(Debug, PartialEq, Eq)]
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


  fn to_bytes(&self) -> [u8; Header::SIZE] {
    let mut bytes = [0u8; Header::SIZE];
    bytes[0..8].copy_from_slice(&self.total_size.to_le_bytes());
    bytes[8..16].copy_from_slice(&self.keys.to_le_bytes());
    bytes[16..24].copy_from_slice(&self.lookup_start.to_le_bytes());
    bytes[24..32].copy_from_slice(&self.start_data.to_le_bytes());
    bytes[32..40].copy_from_slice(&self.offset_free.to_le_bytes());
    bytes
  }

  fn from_bytes(bytes: &[u8]) -> Self {
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

enum EntryState {
  Used = 0,
  Deleted = 1,
}

struct HashEntry {
  size_key: u64,
  key: [u8; 1024],
  data_offset: u64,
  size: u64,
  state: EntryState,
}

impl HashEntry {
  const SIZE: usize = 8 + 1024 + 8 + 8 + 1;

  fn to_bytes(&self) -> [u8; HashEntry::SIZE] {
    let mut bytes = [0u8; HashEntry::SIZE];
    bytes[0..8].copy_from_slice(&self.size_key.to_le_bytes());
    bytes[8..1032].copy_from_slice(&self.key);
    bytes[1032..1040].copy_from_slice(&self.data_offset.to_le_bytes());
    bytes[1040..1048].copy_from_slice(&self.size.to_le_bytes());
    bytes[1048] = match self.state {
      EntryState::Used => 0,
      EntryState::Deleted => 1,
    };
    bytes
  }

  fn from_bytes(bytes: &[u8]) -> Self {
    let size_key = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
    let key = bytes[8..1032].try_into().unwrap();
    let data_offset = u64::from_le_bytes(bytes[1032..1040].try_into().unwrap());
    let size = u64::from_le_bytes(bytes[1040..1048].try_into().unwrap());
    let state = match bytes[1048] {
      0 => EntryState::Used,
      1 => EntryState::Deleted,
      _ => panic!("Invalid entry state"),
    };
    HashEntry {
      size_key,
      key,
      data_offset,
      size,
      state,
    }
  }

  fn is_used(&self) -> bool {
    matches!(self.state, EntryState::Used)
  }
}


struct BTreeHeader {
  root_id: u64,
  next_id: u64,
  
}

pub struct Storage {
  path: PathBuf,
  header: Header,
  mmap: memmap2::MmapMut,
  mmap1kb: memmap2::MmapMut,
  mmap10kb: memmap2::MmapMut,
  mmap100kb: memmap2::MmapMut,
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

    let mut file1kb = std::fs::OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .open(data_path.clone().join("1kb.store"))?;

    let mut file10kb = std::fs::OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .open(data_path.clone().join("10kb.store"))?;

    let mut file100kb = std::fs::OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .open(data_path.clone().join("100kb.store"))?;

    let metadata = file.metadata().unwrap();

    if metadata.len() < Header::SIZE as u64 {
      file.set_len(initial_size)?;
    }

    let mut mmap = unsafe { memmap2::MmapMut::map_mut(&file)? };
    let mut mmap1kb = unsafe { memmap2::MmapMut::map_mut(&file1kb)? };
    let mut mmap10kb = unsafe { memmap2::MmapMut::map_mut(&file10kb)? };
    let mut mmap100kb = unsafe { memmap2::MmapMut::map_mut(&file100kb)? };

    let check = Header::from_bytes(&mmap[0..Header::SIZE]);

    let header = if check.total_size == 0 && check.keys == 0 {
      let header = Header {
        total_size: initial_size,
        keys: 0 as u64,
        lookup_start: Header::SIZE as u64,
        start_data: Header::SIZE as u64 + 1024 * 1024 * 1024,
        offset_free: Header::SIZE as u64 + 1024 * 1024 * 1024,
      };
      let header_bytes = header.to_bytes();
      (&mut mmap[0..Header::SIZE]).copy_from_slice(&header_bytes);
      mmap.flush()?;
      header
    } else {
      check
    };

    Ok(Storage {
      path,
      header,
      mmap,
      mmap1kb,
      mmap10kb,
      mmap100kb,
    })
  }

  pub fn get_header(&self) -> &Header {
    &self.header
  }

  pub fn add(&mut self, key: &[u8], data: &[u8]) -> std::io::Result<()> {
    let position = self.header.offset_free;

    if key.len() > 1024 {
      return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Key size exceeds maximum length"));
    }

    let mut buf =  [0u8; 1024];

    buf[..key.len()].copy_from_slice(&key);

    let entry = HashEntry {
      size_key: key.len() as u64,
      key: buf,
      data_offset: position,
      size: data.len() as u64,
      state: EntryState::Used,
    };


    self.mmap[position as usize..(position + entry.size) as usize].copy_from_slice(data);

    let p_keys = |n| self.header.lookup_start + ((self.header.keys + n) * HashEntry::SIZE as u64);

    self.mmap[p_keys(0) as usize..p_keys(1) as usize].copy_from_slice(&entry.to_bytes());

    self.header.offset_free += entry.size;
    self.header.keys += 1;

    let _ = self.mmap.flush_async();

    Ok(())
  }

  pub fn search(&self, key: &[u8]) -> Option<&[u8]> {
    let mut offset = self.header.lookup_start;

    while offset < self.header.start_data {
      let entry = HashEntry::from_bytes(&self.mmap[offset as usize..(offset + HashEntry::SIZE as u64) as usize]);
      if &entry.key[0..entry.size_key as usize] == key && entry.is_used() {
        return Some(&self.mmap[entry.data_offset as usize..(entry.data_offset + entry.size) as usize]);
      }
      offset += HashEntry::SIZE as u64;
    }

    None
  }

  pub fn search_p(&self, key: &[u8]) -> Option<Vec<u8>> {
    let mut buff: Vec<u8> = Vec::new();
    let mut offset = self.header.lookup_start;

    while offset < self.header.start_data {
      let entry = HashEntry::from_bytes(&self.mmap[offset as usize..(offset + HashEntry::SIZE as u64) as usize]);
      if &entry.key[0..key.len() as usize] == key && entry.is_used() {
        buff.append(&mut self.mmap[entry.data_offset as usize..(entry.data_offset + entry.size) as usize].to_vec());
        buff.append(b"\r".to_vec().as_mut());
      }
      offset += HashEntry::SIZE as u64;
    }

    if buff.len() > 0 {
      Some(buff)
    } else {
      None
    }
  }

  pub fn get(&mut self, data_offset: u64, data_size: u64) -> &[u8] {
    &self.mmap[data_offset as usize..(data_offset + data_size) as usize]
  }

  pub fn del(&mut self, key: &[u8]) -> std::io::Result<()> {
    let mut offset = self.header.lookup_start;

    while offset < self.header.start_data {
      let mut entry = HashEntry::from_bytes(&self.mmap[offset as usize..(offset + HashEntry::SIZE as u64) as usize]);
      if &entry.key[0..entry.size_key as usize] == key && entry.is_used() {
        entry.state = EntryState::Deleted;
        self.mmap[offset as usize..(offset + HashEntry::SIZE as u64) as usize].copy_from_slice(&entry.to_bytes());
        self.mmap.flush()?;
        return Ok(());
      }
      offset += HashEntry::SIZE as u64;
    }

    Err(std::io::Error::new(std::io::ErrorKind::NotFound, "Key not found"))
  }
}


#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_storage() {
    let path = PathBuf::from("test.db");
    let initial_size = 1024 * 1024 * 10; // 10 MB
    let mut storage = Storage::open(path, initial_size).unwrap();

    {
      let header = storage.get_header();
      assert_eq!(header, &Header {
        total_size: initial_size,
        keys: 0,
        lookup_start: Header::SIZE as u64,
        start_data: Header::SIZE as u64 + 1024 * 1024,
        offset_free: Header::SIZE as u64 + 1024 * 1024,
      });
    }

    let key = *b"test1";
    let data = b"Hello, world!";
    storage.add(&key, data).unwrap();
    let retrieved = storage.search(&key).unwrap();
    assert_eq!(retrieved, b"Hello, world!");

  }
}