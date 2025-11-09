
struct Header {
  total_size: u64,

  lookup_start: u64,

  start_data: u64,
  offset_free: u64,
}

enum EntryState {
  Used = 0,
  Deleted = 1,
}

struct HashEntry {
  key: u64,
  data_offset: u64,
  size: u64,
  state: EntryState,
}