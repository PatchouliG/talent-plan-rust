#[cfg(test)]
mod test {
    use log::{info, warn};
    use rand;
    use rand::{SeedableRng, Rng};
    use rand::rngs::StdRng;
    use tempfile::TempDir;


    #[test]
    fn test() {
        let mut rng = StdRng::seed_from_u64(23);
        for i in 0..10 {
            let r = rng.gen_range(0..10);
            println!("{}", r);
        }
    }

    #[test]
    fn debug() {
        let temp_dir = TempDir::new().unwrap();

        let tree = sled::open(temp_dir.path()).unwrap();
        let old_value = tree.insert("key", "value").unwrap();
        // let v = tree.get("key").unwrap().unwrap();
        assert_eq!(
            tree.get(&"key").unwrap(),
            Some(sled::IVec::from("value")),
        );
        let v = tree.get(&"key").unwrap().unwrap();
        let t = v.to_vec();
        let r = String::from_utf8_lossy(&t).to_string();
        print!("{}", r);

        tree.remove("key");
    }
}