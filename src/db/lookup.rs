use std::fs::{self, OpenOptions, File};
use std::collections::HashMap;
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use crate::error::{Error, Result};
use std::path::{Path, PathBuf};

#[derive(Debug, Copy, Clone, PartialEq)]
pub(crate) struct EntryLocation {
    pub block: u64,
    pub pointer: u64
}

impl EntryLocation {
    fn bit_offset(&self) -> usize {
        BTREE_BLOCK_SIZE * (self.block as usize) + (self.pointer as usize)
    }
}

pub(crate) struct LookupTable {
    map_file: File,
    map_path: PathBuf,
    map: HashMap<u64, EntryLocation>,
    wal_file: File,
    wal_path: PathBuf,
    wal: Vec<WalOperation>,
}

const BTREE_BLOCK_SIZE: usize = 4096;
const WAL_BLOCK_SIZE: usize = 25;
const MAP_BLOCK_SIZE: usize = 24;

#[derive(Debug, Copy, Clone)]
pub(crate) enum WalOperation {
    Insert{key: u64, location: EntryLocation},
    Remove{key: u64},
}

impl LookupTable {
    pub fn new(folder: &str) -> Result<Self> {
        LookupTable::new_reset(folder, false)
    }

    pub fn new_reset(folder: &str, reset: bool) -> Result<Self> {
        let map_path = Path::new(folder).join("map.db");
        if let Some(parent) = map_path.parent() {
            fs::create_dir_all(parent).expect("Failed to create directory for map.db");
        }
        let wal_path = Path::new(folder).join("wal.db");
        if let Some(parent) = wal_path.parent() {
            fs::create_dir_all(parent).expect("Failed to create directory for wal.db");
        }
        if reset {
            Self::cleanup(map_path.clone(), wal_path.clone())?;
        }
        let mut map_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(map_path.clone())
            ?;

        let mut wal_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(wal_path.clone())
            ?;
        let map = LookupTable::get_map_from_file(&mut map_file)?;
        let wal = LookupTable::get_wal_from_file(&mut wal_file)?;
        Ok(Self {map_file, map_path, map, wal_file, wal_path, wal})
    }

    pub fn add(&mut self, key: u64, location: EntryLocation) -> Result<()> {
        self.map.insert(key, location);
        let wal_operation = WalOperation::Insert{key, location};
        self.wal.push(wal_operation);
        LookupTable::write_wal_operation_to_file(&mut self.wal_file, &wal_operation)?;
        Ok(())
    }

    pub fn remove(&mut self, key: u64) -> Result<()> {
        self.map.remove(&key);
        let wal_operation = WalOperation::Remove{key};
        self.wal.push(wal_operation);
        LookupTable::write_wal_operation_to_file(&mut self.wal_file, &wal_operation)?;
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        LookupTable::write_map_to_file(&mut self.map_file, &self.map)?;
        self.wal.clear();
        self.wal_file.set_len(0)?;
        self.wal_file.sync_all()?;
        Ok(())
    }

    // Utility function to delete map.db and wal.db files
    pub fn cleanup(map_path: PathBuf, wal_path: PathBuf) -> Result<()> {
        if map_path.exists() {
            println!("Removing map file");
            fs::remove_file(map_path.to_str().unwrap())?;
        }
        if wal_path.exists() {
            println!("Removing wal file");
            fs::remove_file(wal_path.to_str().unwrap())?;
        }
        Ok(())
    }

    fn get_map_from_file(file: &mut File) -> Result<HashMap<u64, EntryLocation>> {
        let file_size = file.metadata()?.len() as usize;
        let mut reader = BufReader::new(file);
        let mut buffer = Vec::with_capacity(file_size);
        let mut hashmap = HashMap::new();

        reader.read_to_end(&mut buffer)?;
        for chunk in buffer.chunks_exact(MAP_BLOCK_SIZE) {
            let key = u64::from_le_bytes(chunk[0..8].try_into()?);
            let block = u64::from_le_bytes(chunk[8..16].try_into()?);
            let pointer = u64::from_le_bytes(chunk[16..24].try_into()?);
            hashmap.insert(key, EntryLocation { block, pointer });
        }
        Ok(hashmap)
    }

    fn get_wal_from_file(file: &mut File) -> Result<Vec<WalOperation>> {
        let file_size = file.metadata()?.len() as usize;
        let mut reader = BufReader::new(file);
        let mut buffer = Vec::with_capacity(file_size);
        let mut wal = Vec::new();
        reader.read_to_end(&mut buffer)?;
        for chunk in buffer.chunks_exact(WAL_BLOCK_SIZE) {
            let op_type = chunk[0];
            let key = u64::from_le_bytes(chunk[1..9].try_into()?);
            if op_type == 0 {
                let block = u64::from_le_bytes(chunk[9..17].try_into()?);
                let pointer = u64::from_le_bytes(chunk[17..25].try_into()?);
                wal.push(WalOperation::Insert{key, location: EntryLocation { block, pointer }});
            } else if op_type == 1 {
                wal.push(WalOperation::Remove{key});
            }
        }
        Ok(wal)
    }

    fn write_map_to_file(file: &mut File, map: &HashMap<u64, EntryLocation>) -> Result<()> {
        file.seek(SeekFrom::Start(0))?;
        file.set_len(0)?;
        for (key, location) in map {
            let mut buffer = vec![0; MAP_BLOCK_SIZE];
            buffer[0..8].copy_from_slice(&key.to_le_bytes());
            buffer[8..16].copy_from_slice(&location.block.to_le_bytes());
            buffer[16..24].copy_from_slice(&location.pointer.to_le_bytes());
            file.write_all(&buffer)?;
        }
        file.sync_all()?;
        Ok(())
    }

    fn write_wal_operation_to_file(file: &mut File, operation: &WalOperation) -> Result<()> {
        let mut buffer = vec![0; WAL_BLOCK_SIZE];
        match operation {
            WalOperation::Insert{key, location} => {
                buffer[0] = 0;
                buffer[1..9].copy_from_slice(&key.to_le_bytes());
                buffer[9..17].copy_from_slice(&location.block.to_le_bytes());
                buffer[17..25].copy_from_slice(&location.pointer.to_le_bytes());
            }
            WalOperation::Remove{key} => {
                buffer[0] = 1;
                buffer[1..9].copy_from_slice(&(*key as u64).to_le_bytes());
            }
        }
        file.write_all(&buffer)?;
        file.sync_all()?;
        Ok(())
    }

    fn get(&self, key: u64) -> Result<Option<EntryLocation>> {
        Ok(self.map.get(&key).cloned())
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[test]
    #[serial]
    fn test_add() -> Result<()> {
        let mut lt = LookupTable::new_reset("test", true)?;
        let el1= EntryLocation { block: 0, pointer: 0 };
        let el2= EntryLocation { block: 0, pointer: 1 };
        lt.add(1, el1)?;
        lt.add(2, el2)?;

        let el1_actual = lt.get(1)?;
        let el2_actual = lt.get(2)?;
        assert_eq!(Some(el1), el1_actual);
        assert_eq!(Some(el2), el2_actual);
        LookupTable::cleanup(lt.map_path, lt.wal_path)?;
        Ok(())
    }

    #[test]
    #[serial]
    fn test_remove() -> Result<()>{
        let mut lt = LookupTable::new_reset("test", true)?;
        let el1= EntryLocation { block: 0, pointer: 0 };
        let el2= EntryLocation { block: 0, pointer: 1 };
        lt.add(1, el1)?;
        lt.add(2, el2)?;
        lt.remove(1)?;

        assert_eq!(lt.map.len(), 1);
        assert_eq!(lt.map.get(&1), None);
        LookupTable::cleanup(lt.map_path, lt.wal_path)?;
        Ok(())
    }

    #[test]
    #[serial]
    fn test_flush() -> Result<()> {
        let mut lt = LookupTable::new_reset("test", true)?;
        let el1= EntryLocation { block: 0, pointer: 0 };
        let el2= EntryLocation { block: 0, pointer: 1 };
        lt.add(1, el1)?;
        lt.add(2, el2)?;
        lt.remove(1)?;
        lt.flush()?;
        assert_eq!(lt.get(1)?, None);
        assert_eq!(lt.get(2)?, Some(EntryLocation { block: 0, pointer: 1 }));
        assert_eq!(lt.map.len(), 1);
        let lt2 = LookupTable::new("test")?;
        println!("{:?}", lt2.map);
        assert_eq!(lt2.map.get(&1), None);
        assert_eq!(lt2.get(2)?, Some(EntryLocation { block: 0, pointer: 1 }));
        assert_eq!(lt2.map.len(), 1);

        LookupTable::cleanup(lt.map_path, lt.wal_path)?;
        LookupTable::cleanup(lt2.map_path, lt2.wal_path)?;
        Ok(())
    }
}



// Example tests that override error and result
// mod tests {
//     type Error = Box<dyn std::error::Error>;
//     type Result<T> = core::result::Result<T, Error>;

//     // we are overwriting Error and Result here
//     use super::*;

//     #[test]
//     fn test_name() -> Result<()> {
//         // Setup & Fixtures

//         // Exec

//         // Check
//         Ok(())
//     }
// }