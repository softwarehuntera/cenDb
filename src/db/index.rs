use crate::db::lookup::LookupTable;
use crate::error::{Error, Result};

pub struct Index {
    lookup_table: LookupTable,
}

impl Index {
    pub fn new(name: String) -> Result<Self> {
        let lookup_table = LookupTable::new(&name)?;
        Ok( Self { lookup_table } )
    }
}