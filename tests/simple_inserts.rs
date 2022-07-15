use bytes::Bytes;
use jammdb::{Bucket, Data, Error, DB};
use rand::prelude::*;

mod common;

#[test]
fn small_insert() -> Result<(), Error> {
    test_insert((0..=100).collect())?;
    test_insert((0..=100).collect())?;
    test_insert((0..=100).collect())?;
    Ok(())
}

#[test]
fn medium_insert() -> Result<(), Error> {
    test_insert((0..=1000).collect())?;
    test_insert((0..=1000).collect())?;
    test_insert((0..=1000).collect())?;
    Ok(())
}

#[test]
fn large_insert() -> Result<(), Error> {
    test_insert((0..=50000).collect())?;
    test_insert((0..=50000).collect())?;
    test_insert((0..=50000).collect())?;
    Ok(())
}

#[test]
fn test_repeated_insert_and_commit_5k_elements() {
    let random_file = common::RandomFile::new();
    let db = DB::open(&random_file.path).unwrap();

    // Test takes around 98 secs on an Apple M1 chip 16GB RAM
    for _ in 0..5000 {
        let tx = db.tx(true).unwrap();
        let bucket = tx.get_or_create_bucket("mulanbucket").unwrap();
        let next_int = bucket.next_int();
        let data = if next_int % 2 == 0 {
            Bytes::from("mulanstuff")
        } else {
            Bytes::from("mulanstuffstuff")
        };
        bucket.put(next_int.to_be_bytes(), data.as_ref()).unwrap();
        tx.commit().unwrap();
    }

    let tx = db.tx(false).unwrap();
    let bucket = tx.get_bucket("mulanbucket").unwrap();

    let kv = bucket.get_kv((1234 as u64).to_be_bytes()).unwrap();
    assert_eq!(kv.value(), b"mulanstuff");
    let kv = bucket.get_kv((1236 as u64).to_be_bytes()).unwrap();
    assert_eq!(kv.value(), b"mulanstuff");
    let kv = bucket.get_kv((1233 as u64).to_be_bytes()).unwrap();
    assert_eq!(kv.value(), b"mulanstuffstuff");
    let kv = bucket.get_kv((1235 as u64).to_be_bytes()).unwrap();
    assert_eq!(kv.value(), b"mulanstuffstuff");
    let kv = bucket.get_kv((1033 as u64).to_be_bytes()).unwrap();
    assert_eq!(kv.value(), b"mulanstuffstuff");
    let kv = bucket.get_kv((4777 as u64).to_be_bytes()).unwrap();
    assert_eq!(kv.value(), b"mulanstuffstuff");
}

#[test]
fn test_repeated_insert_then_commit_5k_elements() {
    let random_file = common::RandomFile::new();
    let db = DB::open(&random_file.path).unwrap();

    let tx = db.tx(true).unwrap();
    let bucket = tx.get_or_create_bucket("mulanbucket").unwrap();

    for _ in 0..5000 {
        let next_int = bucket.next_int();
        let data = if next_int % 2 == 0 {
            Bytes::from("mulanstuff")
        } else {
            Bytes::from("mulanstuffstuff")
        };
        bucket.put(next_int.to_be_bytes(), data.as_ref()).unwrap();
    }

    tx.commit().unwrap();

    let tx = db.tx(false).unwrap();
    let bucket = tx.get_bucket("mulanbucket").unwrap();

    let kv = bucket.get_kv((1234 as u64).to_be_bytes()).unwrap();
    assert_eq!(kv.value(), b"mulanstuff");
    let kv = bucket.get_kv((1236 as u64).to_be_bytes()).unwrap();
    assert_eq!(kv.value(), b"mulanstuff");
    let kv = bucket.get_kv((1233 as u64).to_be_bytes()).unwrap();
    assert_eq!(kv.value(), b"mulanstuffstuff");
    let kv = bucket.get_kv((1235 as u64).to_be_bytes()).unwrap();
    assert_eq!(kv.value(), b"mulanstuffstuff");
    let kv = bucket.get_kv((1033 as u64).to_be_bytes()).unwrap();
    assert_eq!(kv.value(), b"mulanstuffstuff");
    let kv = bucket.get_kv((4777 as u64).to_be_bytes()).unwrap();
    assert_eq!(kv.value(), b"mulanstuffstuff");
}

#[test]
fn test_repeated_insert_and_commit_50k_elements() {
    let random_file = common::RandomFile::new();
    let db = DB::open(&random_file.path).unwrap();

    // Test takes around 1034 secs on an Apple M1 chip 16GB RAM
    for _ in 0..=50000 {
        let tx = db.tx(true).unwrap();
        let bucket = tx.get_or_create_bucket("mulanbucket").unwrap();
        let next_int = bucket.next_int();
        let data = if next_int % 2 == 0 {
            Bytes::from("mulanstuff")
        } else {
            Bytes::from("mulanstuffstuff")
        };
        bucket.put(next_int.to_be_bytes(), data.as_ref()).unwrap();
        tx.commit().unwrap();
    }

    let tx = db.tx(false).unwrap();
    let bucket = tx.get_bucket("mulanbucket").unwrap();

    let kv = bucket.get_kv((0 as u64).to_be_bytes()).unwrap();
    assert_eq!(kv.value(), b"mulanstuff");
    let kv = bucket.get_kv((41234 as u64).to_be_bytes()).unwrap();
    assert_eq!(kv.value(), b"mulanstuff");
    let kv = bucket.get_kv((1236 as u64).to_be_bytes()).unwrap();
    assert_eq!(kv.value(), b"mulanstuff");
    let kv = bucket.get_kv((50000 as u64).to_be_bytes()).unwrap();
    assert_eq!(kv.value(), b"mulanstuff");
    let kv = bucket.get_kv((41233 as u64).to_be_bytes()).unwrap();
    assert_eq!(kv.value(), b"mulanstuffstuff");
    let kv = bucket.get_kv((41235 as u64).to_be_bytes()).unwrap();
    assert_eq!(kv.value(), b"mulanstuffstuff");
    let kv = bucket.get_kv((1033 as u64).to_be_bytes()).unwrap();
    assert_eq!(kv.value(), b"mulanstuffstuff");
    let kv = bucket.get_kv((27777 as u64).to_be_bytes()).unwrap();
    assert_eq!(kv.value(), b"mulanstuffstuff");
}

#[test]
fn test_repeated_insert_then_commit_50k_elements() {
    let random_file = common::RandomFile::new();
    let db = DB::open(&random_file.path).unwrap();

    let tx = db.tx(true).unwrap();
    let bucket = tx.get_or_create_bucket("mulanbucket").unwrap();

    for _ in 0..=50000 {
        let next_int = bucket.next_int();
        let data = if next_int % 2 == 0 {
            Bytes::from("mulanstuff")
        } else {
            Bytes::from("mulanstuffstuff")
        };
        bucket.put(next_int.to_be_bytes(), data.as_ref()).unwrap();
    }

    tx.commit().unwrap();

    let tx = db.tx(false).unwrap();
    let bucket = tx.get_bucket("mulanbucket").unwrap();

    let kv = bucket.get_kv((0 as u64).to_be_bytes()).unwrap();
    assert_eq!(kv.value(), b"mulanstuff");
    let kv = bucket.get_kv((41234 as u64).to_be_bytes()).unwrap();
    assert_eq!(kv.value(), b"mulanstuff");
    let kv = bucket.get_kv((1236 as u64).to_be_bytes()).unwrap();
    assert_eq!(kv.value(), b"mulanstuff");
    let kv = bucket.get_kv((50000 as u64).to_be_bytes()).unwrap();
    assert_eq!(kv.value(), b"mulanstuff");
    let kv = bucket.get_kv((41233 as u64).to_be_bytes()).unwrap();
    assert_eq!(kv.value(), b"mulanstuffstuff");
    let kv = bucket.get_kv((41235 as u64).to_be_bytes()).unwrap();
    assert_eq!(kv.value(), b"mulanstuffstuff");
    let kv = bucket.get_kv((1033 as u64).to_be_bytes()).unwrap();
    assert_eq!(kv.value(), b"mulanstuffstuff");
    let kv = bucket.get_kv((27777 as u64).to_be_bytes()).unwrap();
    assert_eq!(kv.value(), b"mulanstuffstuff");
}

fn test_insert(mut values: Vec<u64>) -> Result<(), Error> {
    let random_file = common::RandomFile::new();
    let mut rng = rand::thread_rng();
    {
        let db = DB::open(&random_file.path)?;
        {
            let tx = db.tx(true)?;
            let b = tx.create_bucket("abc")?;
            // insert data in a random order
            values.shuffle(&mut rng);
            for i in values.iter() {
                let existing = b.put(i.to_be_bytes(), i.to_string())?;
                assert!(existing.is_none());
            }
            // check before commit
            check_data(&b, values.len() as u64, 1);
            assert_eq!(b.next_int(), values.len() as u64);
            tx.commit()?;
        }
        {
            let tx = db.tx(false)?;
            let b = tx.get_bucket("abc")?;
            // check after commit before closing file
            check_data(&b, values.len() as u64, 1);
            assert_eq!(b.next_int(), values.len() as u64);
        }
    }
    {
        let db = DB::open(&random_file.path)?;
        let tx = db.tx(false)?;
        let b = tx.get_bucket("abc")?;
        // check after re-opening file
        check_data(&b, values.len() as u64, 1);
        assert_eq!(b.next_int(), values.len() as u64);
        let missing_key = (values.len() + 1) as u64;
        assert!(b.get(missing_key.to_be_bytes()).is_none());
    }
    let db = DB::open(&random_file.path)?;
    db.check()
}

fn check_data(b: &Bucket, len: u64, repeats: usize) {
    let mut count: u64 = 0;
    for (i, data) in b.cursor().into_iter().enumerate() {
        let i = i as u64;
        count += 1;
        match &*data {
            Data::KeyValue(kv) => {
                assert_eq!(kv.key(), i.to_be_bytes());
                assert_eq!(kv.value(), i.to_string().repeat(repeats).as_bytes());
            }
            _ => panic!("Expected Data::KeyValue"),
        };
    }
    assert_eq!(count, len);
}
