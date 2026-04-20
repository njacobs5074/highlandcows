#![allow(dead_code)]

use highlandcows_isam::{DeriveKey, Isam};
use serde::{Deserialize, Serialize};
use tempfile::TempDir;

/// Open a fresh database in a temp dir.
pub fn make_db<K, V>() -> (TempDir, Isam<K, V>)
where
    K: serde::Serialize + serde::de::DeserializeOwned + Ord + Clone + 'static,
    V: serde::Serialize + serde::de::DeserializeOwned + Clone + 'static,
{
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test");
    let db = Isam::create(&path).unwrap();
    (dir, db)
}

// ── Person / secondary index types ────────────────────────────────────────── //

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Person {
    pub name: String,
    pub city: String,
}

pub struct CityIndex;
impl DeriveKey<Person> for CityIndex {
    type Key = String;
    fn derive(p: &Person) -> String {
        p.city.clone()
    }
}

pub struct NameIndex;
impl DeriveKey<Person> for NameIndex {
    type Key = String;
    fn derive(p: &Person) -> String {
        p.name.clone()
    }
}

/// Open a fresh person database with `city` and `name` indices.
pub fn make_person_db() -> (TempDir, Isam<u32, Person>) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("people");
    let db = Isam::<u32, Person>::builder()
        .with_index("city", CityIndex)
        .with_index("name", NameIndex)
        .create(&path)
        .unwrap();
    (dir, db)
}
