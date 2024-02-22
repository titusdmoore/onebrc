use fnv::FnvHashMap;
use std::cmp::min;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Cursor};
use std::os::fd::AsRawFd;
use std::str;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;

use std::io::prelude::*;

extern "C" {
    fn mmap(addr: *mut u8, length: usize, prot: i32, flags: i32, fd: i32, offset: i64) -> *mut u8;
    fn lseek(fd: i32, offset: i64, whence: i32) -> i64;
}

const MAP_PRIVATE: i32 = 0x02;
const PROT_READ: i32 = 0x01;
const MAP_FAILED: *mut u8 = !0 as *mut u8;

const THREAD_COUNT: usize = 25;

pub fn main() -> io::Result<()> {
    // run_parse_1fnv()
    run_parse()
}

pub fn run_parse() -> io::Result<()> {
    let file = File::open("measurements.txt")?;
    let file_size = unsafe { lseek(file.as_raw_fd(), 0, 2) };
    let mut entries: HashMap<String, Vec<f64>> = HashMap::with_capacity(10000);
    let (tx, rx) = mpsc::channel();
    let sender = Arc::new(Mutex::new(tx));

    let mmap_ptr = unsafe {
        mmap(
            std::ptr::null_mut(),
            file_size as usize,
            PROT_READ,
            MAP_PRIVATE,
            file.as_raw_fd(),
            0,
        )
    };

    if mmap_ptr == MAP_FAILED {
        return Err(io::Error::last_os_error());
    }

    // Convert the raw pointer to a slice
    let mmap_slice =
        unsafe { std::slice::from_raw_parts(mmap_ptr as *const u8, file_size as usize) };

    let chunk_size = file_size / THREAD_COUNT as i64;
    let mut handles = vec![];

    let mut head = 0;
    for i in 0..THREAD_COUNT {
        let end = min(
            find_next_newline(mmap_slice, (head + chunk_size) as usize) + 1,
            (file_size) as usize,
        );
        let thread_mmap_slice = Cursor::new(&mmap_slice[head as usize..end]);
        let sender = Arc::clone(&sender);

        println!("Running thread {} with section {} to {}", i, head, end - 1);

        let handle = thread::spawn(move || {
            println!("Thread started: {}", i);
            let mut thread_hashmap: HashMap<String, Vec<f64>> = HashMap::with_capacity(10000);

            for line in thread_mmap_slice.split(b'\n').flatten() {
                // let entry: Vec<&str> = str::from_utf8(&line).unwrap().split(';').collect();
                let (location, str_temp) =
                    cheat_split_once(str::from_utf8(&line).unwrap(), b';').unwrap();

                let entry_value: f64 = match str_temp.parse() {
                    Ok(num) => num,
                    Err(_) => {
                        println!("Error: {} is not a number", str_temp);
                        panic!();
                    }
                };

                thread_hashmap
                    .entry(location.to_string())
                    .and_modify(|curr_vec| curr_vec.push(entry_value))
                    .or_insert_with(|| {
                        let mut new_vec = Vec::with_capacity(10000);
                        new_vec.push(entry_value);
                        new_vec
                    });
            }

            let sender = sender.lock().unwrap();
            sender.send(thread_hashmap).unwrap();
            println!("Thread finished: {}", i);
        });

        head = end as i64;
        handles.push(handle);

        if head >= file_size {
            break;
        }
    }

    for _ in 0..THREAD_COUNT {
        let received = rx.recv();

        if let Ok(received) = received {
            for (key, value) in received.iter() {
                entries
                    .entry(key.to_string())
                    .and_modify(|curr_vec| curr_vec.extend(value.clone()))
                    .or_insert(value.clone());
            }
        }
    }

    for handle in handles {
        handle.join().unwrap();
    }

    for (key, value) in entries.iter() {
        let sum: f64 = value.iter().sum();
        let avg: f64 = ((sum / value.len() as f64) * 100.0).round() / 100.0;
        println!("{}: {}", key, avg);
    }

    Ok(())
}

pub fn run_parse_1fnv() -> io::Result<()> {
    let file = File::open("measurements.txt")?;
    let file_size = unsafe { lseek(file.as_raw_fd(), 0, 2) };
    let mut entries: FnvHashMap<_, Vec<f64>> =
        FnvHashMap::with_capacity_and_hasher(10000, Default::default());
    let (tx, rx) = mpsc::channel();
    let sender = Arc::new(Mutex::new(tx));

    let mmap_ptr = unsafe {
        mmap(
            std::ptr::null_mut(),
            file_size as usize,
            PROT_READ,
            MAP_PRIVATE,
            file.as_raw_fd(),
            0,
        )
    };

    if mmap_ptr == MAP_FAILED {
        return Err(io::Error::last_os_error());
    }

    // Convert the raw pointer to a slice
    let mmap_slice =
        unsafe { std::slice::from_raw_parts(mmap_ptr as *const u8, file_size as usize) };

    let chunk_size = file_size / THREAD_COUNT as i64;
    let mut handles = vec![];

    let mut head = 0;
    for i in 0..THREAD_COUNT {
        let end = min(
            find_next_newline(mmap_slice, (head + chunk_size) as usize) + 1,
            (file_size) as usize,
        );
        let thread_mmap_slice = Cursor::new(&mmap_slice[head as usize..end]);
        let sender = Arc::clone(&sender);

        println!("Running thread {} with section {} to {}", i, head, end - 1);

        let handle = thread::spawn(move || {
            println!("Thread started: {}", i);
            let mut thread_hashmap: FnvHashMap<_, Vec<f64>> =
                FnvHashMap::with_capacity_and_hasher(10000, Default::default());

            for line in thread_mmap_slice.split(b'\n').flatten() {
                let entry: Vec<&str> = str::from_utf8(&line).unwrap().split(';').collect();

                let entry_value: f64 = match entry[1].parse() {
                    Ok(num) => num,
                    Err(_) => {
                        println!("Error: {} is not a number", entry[1]);
                        panic!();
                    }
                };

                thread_hashmap
                    .entry(entry[0].to_string())
                    .and_modify(|curr_vec| curr_vec.push(entry_value))
                    .or_insert_with(|| {
                        let mut new_vec = Vec::with_capacity(10000);
                        new_vec.push(entry_value);
                        new_vec
                    });
            }

            let sender = sender.lock().unwrap();
            sender.send(thread_hashmap).unwrap();
            println!("Thread finished: {}", i);
        });

        head = end as i64;
        handles.push(handle);

        if head >= file_size {
            break;
        }
    }

    for _ in 0..THREAD_COUNT {
        let received = rx.recv();

        if let Ok(received) = received {
            for (key, value) in received.iter() {
                entries
                    .entry(key.to_string())
                    .and_modify(|curr_vec| curr_vec.extend(value.clone()))
                    .or_insert(value.clone());
            }
        }
    }

    for handle in handles {
        handle.join().unwrap();
    }

    for (key, value) in entries.iter() {
        let sum: f64 = value.iter().sum();
        let avg: f64 = ((sum / value.len() as f64) * 100.0).round() / 100.0;
        println!("{}: {}", key, avg);
    }

    Ok(())
}

pub fn find_next_newline(mmap_slice: &[u8], start: usize) -> usize {
    for (i, char) in mmap_slice.iter().enumerate().skip(start) {
        if *char == b'\n' {
            return i;
        }
    }

    mmap_slice.len() - 1
}

// This is a bit of a cheat, because we know that the delimiter will always be present, that there
// will be fewer characters after the delimiter than before, and that the delimiter will be a single
// character.
pub fn cheat_split_once(s: &str, delimiter: u8) -> Result<(&str, &str), &str> {
    let mut position = s.len() - 1;

    loop {
        if s.as_bytes()[position] == delimiter {
            return Ok((&s[..position], &s[position + 1..]));
        }

        position -= 1;

        if position == 0 {
            return Err("Delimiter not found");
        }
    }
}
