extern crate rand;

use byteorder::{ByteOrder, LittleEndian};
use std::env;
use std::f32;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::process;
use std::sync::{Arc, Barrier, RwLock};
use std::thread;

use std::io::prelude::*;
use std::io::SeekFrom;

use std::thread::sleep;
use std::time::{Duration, Instant};

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() != 4 {
        println!("Usage:  <threads> input output");
        process::exit(1);
    }

    let threads = args[1].parse::<usize>().unwrap();
    let inp_path = &args[2];
    let out_path = &args[3];

    // Sample
    // Calculate pivots
    let mut inpf = File::open(inp_path).unwrap();
    let size_count = read_size(&mut inpf);
    let mut inpbuffer = Vec::new();

    let now = Instant::now();
    inpf.read_to_end(&mut inpbuffer).unwrap();

    println!("File read time: {}", now.elapsed().as_secs());

    let mut inputdata = Vec::new();

    let now = Instant::now();

    let mut ii = 0;
    while ii < inpbuffer.len() {
        let mut element: [u8; 4] = [0; 4];
        element[0] = inpbuffer[ii];
        element[1] = inpbuffer[ii + 1];
        element[2] = inpbuffer[ii + 2];
        element[3] = inpbuffer[ii + 3];
        inputdata.push(LittleEndian::read_f32(&element));
        ii += 4;
    }

    println!("Input process time: {}", now.elapsed().as_secs());

    let now = Instant::now();

    let pivots = find_pivots(&inputdata, threads, size_count);

    println!("Find pivots time: {}", now.elapsed().as_secs());

    let mut workers = vec![];

    let mut outf = File::create(out_path).unwrap();
    let tmp = size_count.to_ne_bytes();
    outf.write_all(&tmp).unwrap();
    outf.set_len(size_count).unwrap();

    // Spawn worker threads
    let sizes = Arc::new(RwLock::new(vec![0u64; threads]));
    let barrier = Arc::new(Barrier::new(threads));
    let results = Arc::new(RwLock::new(vec![0f32; size_count as usize]));

    for ii in 0..threads {
        let piv = pivots.clone();
        let szs = sizes.clone();
        let bar = barrier.clone();
        let inp_array = inputdata.clone();
        let inp_size = size_count.clone();
        let result_data = results.clone();
        let outpath = out_path.clone();

        let tt = thread::spawn(move || {
            worker(ii, inp_array, piv, szs, bar, inp_size, result_data, outpath);
        });
        workers.push(tt);
    }

    // Join worker threads

    for tt in workers {
        tt.join().unwrap();
    }
}

fn read_size(file: &mut File) -> u64 {
    // TODO: Read size field from data file
    let mut buffer = [0; 8];
    // read up to 8 bytes
    file.read_exact(&mut buffer).unwrap();
    let num = u64::from_ne_bytes(buffer);
    num
}

fn find_pivots(data: &Vec<f32>, threads: usize, num_data: u64) -> Vec<f32> {
    // TODO: Sample 3*(threads-1) items from the file
    // TODO: Sort the sampled list
    let mut pivots = Vec::new();

    let mut index = 0;
    while index < 3 * (threads - 1) {
        let mut rand_no: u64 = rand::random();
        rand_no = rand_no % num_data;
        pivots.push(data[rand_no as usize]); //type convertion
        index += 1
    }

    // Referred from rust official documentation
    pivots.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let mut samples = Vec::new();
    samples.push(0.0);

    let mut ii = 1;

    while ii < pivots.len() {
        samples.push(pivots[ii]);
        ii += 3;
    }
    samples.push(f32::INFINITY);

    samples
}

fn worker(
    tid: usize,
    inp_data: Vec<f32>,
    pivots: Vec<f32>,
    sizes: Arc<RwLock<Vec<u64>>>,
    bb: Arc<Barrier>,
    inp_size: u64,
    results: Arc<RwLock<Vec<f32>>>,
    out_path: String,
) {
    // TODO: Open input as local fh

    let mut localdata = Vec::new();
    let mut ii = 0;
    let inpsize = inp_size as usize;

    let now = Instant::now();

    while ii < inpsize {
        if inp_data[ii] >= pivots[tid] && inp_data[ii] < pivots[tid + 1] {
            localdata.push(inp_data[ii]);
            let mut count = sizes.write().unwrap();
            count[tid] += 1;
        }
        ii = ii + 1;
    }

    println!("Finding between pivots {}", now.elapsed().as_secs());

    //Rust documentation

    let now = Instant::now();

    localdata.sort_by(|a, b| a.partial_cmp(b).unwrap());

    println!("Localdata sort time: {}", now.elapsed().as_secs());

    let mut write_buffer = vec![0u8; 4 * inp_size as usize];
    let mut pos_buffer = 0;
    for xx in localdata {
        let tmp = xx.to_bits().to_ne_bytes();
        for kk in 0..4 {
            write_buffer[pos_buffer] = tmp[kk];
            pos_buffer += 1;
        }
    }

    bb.wait();

    let (mut start, mut k) = (0, 0);

    let count = sizes.read().unwrap();

    while k < tid {
        start += count[k];
        k += 1;
    }
    let offset = 8 + start * 4;

    println!("{}: start {:.4}, count {}", tid, pivots[tid], count[tid]);

    let now = Instant::now();

    write_buffer.truncate((count[tid] * 4) as usize);
    {
        let mut outf = OpenOptions::new()
            .read(true)
            .write(true)
            .open(out_path)
            .unwrap();

        outf.seek(SeekFrom::Start(offset)).unwrap();
        outf.write_all(&write_buffer).unwrap();
    }

    println!("File write time: {}", now.elapsed().as_secs());
}
