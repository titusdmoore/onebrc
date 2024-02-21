use std::cmp::min;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Cursor};
use std::os::fd::AsRawFd;
use std::str;
use std::sync::{Arc, Mutex};
use std::thread;

use std::io::prelude::*;

extern "C" {
    fn mmap(addr: *mut u8, length: usize, prot: i32, flags: i32, fd: i32, offset: i64) -> *mut u8;
    fn lseek(fd: i32, offset: i64, whence: i32) -> i64;
}

const MAP_PRIVATE: i32 = 0x02;
const PROT_READ: i32 = 0x01;
const MAP_FAILED: *mut u8 = !0 as *mut u8;

const THREAD_COUNT: usize = 15;

pub fn main() -> io::Result<()> {
    let file = File::open("measurements.txt")?;
    let file_size = unsafe { lseek(file.as_raw_fd(), 0, 2) };
    let entries: Arc<Mutex<HashMap<String, Vec<f64>>>> = Arc::new(Mutex::new(HashMap::new()));

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
        let entries = Arc::clone(&entries);

        println!("Running thread {} with section {} to {}", i, head, end - 1);

        let handle = thread::spawn(move || {
            println!("Thread started: {}", i);
            for line in thread_mmap_slice.split(b'\n') {
                if let Ok(line) = line {
                    let entry: Vec<&str> = str::from_utf8(&line).unwrap().split(";").collect();

                    let entry_value: f64 = match entry[1].parse() {
                        Ok(num) => num,
                        Err(_) => {
                            println!("Error: {} is not a number", entry[1]);
                            panic!();
                        }
                    };

                    // I am 90% sure that this is causing performance issues because it basically
                    // forces everything back to single threaded. I should split each thread into
                    // its own hashmap and then merge them at the end. 10,000 is less that 1
                    // billion :D
                    entries
                        .lock()
                        .unwrap()
                        .entry(entry[0].to_string())
                        .and_modify(|curr_vec| curr_vec.push(entry_value))
                        .or_insert(vec![entry_value]);
                }
            }
            println!("Thread finished: {}", i);
        });

        head = end as i64;
        handles.push(handle);

        if head >= file_size {
            break;
        }
    }

    // Test the slice
    // assert_eq!(b'\n', mmap_slice[find_next_newline(mmap_slice, 0)]);

    // assert_eq!("\n".as_bytes(), &mmap_slice[10].to_be_bytes());

    // println!("{}", str::from_utf8(&mmap_slice[0..11]).unwrap());
    // println!("{:?}", mmap_slice.len());
    //

    for handle in handles {
        handle.join().unwrap();
    }

    for (key, value) in entries.lock().unwrap().iter() {
        let sum: f64 = value.iter().sum();
        let avg: f64 = sum / value.len() as f64;
        println!("{}: {}", key, avg);
    }

    Ok(())
}

pub fn find_next_newline(mmap_slice: &[u8], start: usize) -> usize {
    for i in start..mmap_slice.len() {
        if mmap_slice[i] == b'\n' {
            return i;
        }
    }

    return mmap_slice.len() - 1;
}
