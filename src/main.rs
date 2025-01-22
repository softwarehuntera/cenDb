use std::fs::{OpenOptions, File};
use std::collections::HashMap;
use std::io::{BufReader, Read, Seek, SeekFrom, Write};

fn main() {
    println!("Hello, world!");
}

#[derive(Debug, Copy, Clone)]
struct EntryLocation {
    block: u64,
    pointer: u64
}

impl EntryLocation {
    fn bit_offset(&self) -> usize {
        BTREE_BLOCK_SIZE * (self.block as usize) + (self.pointer as usize)
    }
}

struct LookupTable {
    map_file: File,
    map: HashMap<u64, EntryLocation>,
    wal_file: File,
    wal: Vec<WalOperation>,
}

const BTREE_BLOCK_SIZE: usize = 4096;
const WAL_BLOCK_SIZE: usize = 25;
const MAP_BLOCK_SIZE: usize = 24;

#[derive(Debug, Copy, Clone)]
enum WalOperation {
    Insert(u64, EntryLocation),
    Remove(u64),
}

impl LookupTable {
    pub fn new() -> Self {
        let mut map_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open("map.db")
            .unwrap();

        let mut wal_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open("map.db")
            .unwrap();
        let map = LookupTable::get_map_from_file(&mut map_file);
        let (wal_file, wal) = LookupTable::get_wal_from_file("wal.db");
        LookupTable {
            map_file,
            map,
            wal_file,
            wal
        }
    }

    pub fn add(&mut self, key: u64, location: EntryLocation) {
        self.map.insert(key, location);
        let wal_operation = WalOperation::Insert(key, location);
        self.wal.push(wal_operation);
        LookupTable::write_wal_operation_to_file(&mut self.wal_file, &wal_operation);
    }

    pub fn flush(&mut self) {
        LookupTable::write_map_to_file(&mut self.map_file, &self.map);
        self.wal.clear();
        self.wal_file.set_len(0).unwrap();
    }

    
    fn initialize_file_and_wal(path: &str) -> (File, Vec<WalOperation>) {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .unwrap();
        let mut reader = BufReader::new(&file);
        reader.seek(SeekFrom::Start(0)).unwrap();
        let file_size = file.metadata().unwrap().len() as usize;
        let mut buffer = vec![0; file_size];
        let mut wal = Vec::new();
        while let Ok(_) = reader.read_exact(&mut buffer) {
            for chunk in buffer.chunks_exact(WAL_BLOCK_SIZE) {
                let op_type = chunk[0];
                let key = u64::from_le_bytes(chunk[1..9].try_into().unwrap());
                if op_type == 0 {
                    let block = u64::from_le_bytes(chunk[9..17].try_into().unwrap());
                    let pointer = u64::from_le_bytes(chunk[17..25].try_into().unwrap());
                    wal.push(WalOperation::Insert(key, EntryLocation { block, pointer }));
                } else if op_type == 1 {
                    wal.push(WalOperation::Remove(key));
                }
            }
        }
        (file, wal)
    }
    
    fn get_map_from_file(file: &mut File) -> HashMap<u64, EntryLocation> {
        let file_size = file.metadata().unwrap().len() as usize;
        let mut reader = BufReader::new(file);
        let mut buffer = vec![0; file_size];
        let mut hashmap = HashMap::new();

        while let Ok(_) = reader.read_exact(&mut buffer) {
            for chunk in buffer.chunks_exact(MAP_BLOCK_SIZE) {
                let key = u64::from_le_bytes(chunk[0..8].try_into().unwrap());
                let block = u64::from_le_bytes(chunk[8..16].try_into().unwrap());
                let pointer = u64::from_le_bytes(chunk[16..24].try_into().unwrap());
                hashmap.insert(key, EntryLocation { block, pointer });
            }
        }
        hashmap
    }

    fn write_map_to_file(file: &mut File, map: &HashMap<u64, EntryLocation>) {
        file.set_len(0).unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        for (key, location) in map {
            let mut buffer = vec![0; MAP_BLOCK_SIZE];
            buffer[0..8].copy_from_slice(&key.to_le_bytes());
            buffer[8..16].copy_from_slice(&location.block.to_le_bytes());
            buffer[16..24].copy_from_slice(&location.pointer.to_le_bytes());
            file.write_all(&buffer).unwrap();
        }
        file.sync_all().unwrap();
    }

    fn write_wal_operation_to_file(file: &mut File, operation: &WalOperation) {
        let mut buffer = vec![0; WAL_BLOCK_SIZE];
        match operation {
            WalOperation::Insert(key, location) => {
                buffer[0] = 0;
                buffer[1..9].copy_from_slice(&key.to_le_bytes());
                buffer[9..17].copy_from_slice(&location.block.to_le_bytes());
                buffer[17..25].copy_from_slice(&location.pointer.to_le_bytes());
            }
            WalOperation::Remove(key) => {
                buffer[0] = 1;
                buffer[1..9].copy_from_slice(&(*key as u64).to_le_bytes());
            }
        }
        file.write_all(&buffer).unwrap();
        file.sync_all().unwrap();
    }



    fn lookup(&self, key: usize) -> EntryLocation {

        // read the block
        // find the key
        // return the location
        EntryLocation {
            block: 0,
            pointer: 0
        }
    }
}
struct Index {
    file: File,
    lookup: HashMap<usize, StorageContainer>
}