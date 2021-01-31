use std::collections::HashMap;

use crate::db::file_manager::ValueIndex;

pub struct DBIndex {
    m: HashMap<String, ValueIndex>
// keytomb todo
}

// not thread safe, need lock
impl DBIndex {
    pub fn new() -> DBIndex {
        DBIndex { m: HashMap::new() }
    }
    pub fn set(&mut self, key: String, valueIndex: ValueIndex) {
        self.m.insert(key, valueIndex);
    }
    pub fn get(&self, key: &str) -> Option<&ValueIndex> {
        self.m.get(key)
    }
    pub fn rm(&mut self, key: &str) {
        self.m.remove(key);
        //     todo update key tomb
    }

    // used by compactor
    pub fn updateIndex(&mut self, key: String, index: ValueIndex) { unimplemented!() }
    pub fn deleteIndex(&mut self, key: String) { unimplemented!() }
    pub fn resetTomb(&mut self) { unimplemented!() }
}
