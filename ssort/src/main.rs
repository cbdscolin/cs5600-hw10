extern crate rand;
use rand::Rng;

use std::env;
use std::f32;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::process;
use std::process::exit;
use std::sync::{Arc, Barrier, RwLock};
use std::thread;

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() != 4 {
        println!("Usage:  <threads> input output");
        process::exit(1);

        //println!("Usage:  <threads> input output", args[0]);
    }

    // println!("arg1 : {} arg2: {}, arg3: {}, arg4: {}", args[0], args[1], args[2], args[3]);

    let threads = args[1].parse::<usize>().unwrap();
    let inp_path = &args[2];
    let out_path = &args[3];

    // Sample
    // Calculate pivots
    let mut inpf = File::open(inp_path).unwrap();
    let size_count = read_size(&mut inpf);
    let mut inpbuffer = Vec::new();
    inpf.read_to_end(&mut inpbuffer).unwrap();

    let mut inputdata = Vec::new();

    let mut ii = 0;
    while ii < inpbuffer.len() {
        let mut element: [u8; 4] = [0; 4];
        element[0] = inpbuffer[ii];
        element[1] = inpbuffer[ii + 1];
        element[2] = inpbuffer[ii + 2];
        element[3] = inpbuffer[ii + 3];
        inputdata.push(f32::from_ne_bytes(element));
        //       println!("ii: {} num: {} ", ii % 4  ,f32::from_ne_bytes(element));
        ii += 4;
    }

    let pivots = find_pivots(&inputdata, threads, size_count);

    let mut workers = vec![];

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

        let tt = thread::spawn(move || {
            worker(ii, inp_array, piv, szs, bar, inp_size, result_data);
        });
        workers.push(tt);
    }

    //println!("After exit: {:?}", sizes);

    // Join worker threads

    for tt in workers {
        tt.join().unwrap();
    }

//    println!("input: {:?}", inputdata);
//    println!("output: {:?}", results);

    // Create output file
    {
        let mut outf = File::create(out_path).unwrap();
        let tmp = size_count.to_ne_bytes();
        outf.write_all(&tmp).unwrap();
        outf.set_len(size_count).unwrap();
        let outdata = results.read().unwrap();
        for xx in & *outdata {
            let tmp = xx.to_ne_bytes();
            outf.write_all(&tmp).unwrap();
        }
        outf.set_len(size_count + 4 * size_count).unwrap();
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

fn read_item(file: &mut File, ii: u64) -> f32 {
    // TODO: Read the ii'th float from data file
    0.0
}

fn sample(file: &mut File, count: usize, size: u64) -> Vec<f32> {
    let mut rng = rand::thread_rng();
    let mut ys = vec![];

    // TODO: Sample 'count' random items from the
    // provided file

    ys
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

    //println!("{:?}", samples);

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
) {
    // TODO: Open input as local fh

    let mut localdata = Vec::new();
    let mut ii = 0;
    let inpsize = inp_size as usize;
    while ii < inpsize {
        if inp_data[ii] >= pivots[tid] && inp_data[ii] < pivots[tid + 1] {
            localdata.push(inp_data[ii]);
            let mut count = sizes.write().unwrap();
            count[tid] += 1;
        }
        ii = ii + 1;
    }

    localdata.sort_by(|a, b| a.partial_cmp(b).unwrap());

    bb.wait();

    //println!("Sizes: {:?}", sizes);

    let (mut start, mut k) = (0, 0);

    let count = sizes.read().unwrap();

    println!("{}: start {:.4}, count {}", tid, pivots[tid], count[tid]);

    while k < tid {
        start += count[k];
        k += 1;
    }
    let end: usize = (start + count[tid] - 1) as usize;

    let (mut m, mut k) = (0, start as usize);
    while k <= end {
        let mut output = results.write().unwrap();
        output[k] = localdata[m];
        m = m + 1;
        k = k + 1
    }

    /*
    // TODO: Scan to collect local data
    let data = vec![0f32, 1f32];

    // TODO: Write local size to shared sizes
    {
        // curly braces to scope our lock guard
    }

    // TODO: Sort local data

    // Here's our printout

    // TODO: Write data to local buffer
    let mut cur = Cursor::new(vec![]);

    for xx in &data {
        let tmp = xx.to_ne_bytes();
        cur.write_all(&tmp).unwrap();
    }

    // TODO: Get position for output file
    let prev_count = {
        // curly braces to scope our lock guard
        5
    };

    /*
    let mut outf = OpenOptions::new()
        .read(true)
        .write(true)
        .open(out_path).unwrap();
    */
    // TODO: Seek and write local buffer.

    // TODO: Figure out where the barrier goes.
    */
}
