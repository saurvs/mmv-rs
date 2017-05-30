extern crate memmap;
extern crate byteorder;
extern crate time;
extern crate nix;
#[macro_use]
extern crate bitflags;

use memmap::{Mmap, Protection, MmapViewSync};
use byteorder::{LittleEndian, WriteBytesExt};
use std::fs::OpenOptions;
use std::io::{Cursor, Write};
use std::mem::transmute;
use nix::unistd::getpid;

const HDR_LEN: u64 = 40;
const TOC_BLOCK_LEN: u64 = 16;
const METRIC_BLOCK_LEN: u64 = 104;
const VALUE_BLOCK_LEN: u64 = 32;
const STRING_BLOCK_LEN: u64 = 256;
const METRIC_NAME_MAX_LEN: u64 = 64;

bitflags! {
    pub struct MMVFlags: u32 {
        const NOPREFIX = 1;
        const PROCESS = 2;
        const SENTINEL = 4;
    }
}

#[derive(Copy, Clone)]
pub enum MetricSem {
    Counter = 1,
    Instant = 3,
    Discrete = 4
}

#[derive(Copy, Clone)]
pub enum MetricType {
    I64(i64),
    F64(f64)
}

pub struct Metric {
    name: String,
    item: u32,
    sem: MetricSem,
    indom: u32,
    dim: u32,
    shorttext: String,
    longtext: String,
    val: MetricType,
    mmap_view: Option<MmapViewSync>
}

impl Metric {
    pub fn new(
        name: &str, item: u32, sem: MetricSem,
        indom: u32, dim: u32, init_val: MetricType,
        shorthelp: &str, longhelp: &str) -> Self {
        
        assert!(name.len() < METRIC_NAME_MAX_LEN as usize);
        assert!(shorthelp.len() < STRING_BLOCK_LEN as usize);
        assert!(longhelp.len() < STRING_BLOCK_LEN as usize);

        Metric {
            name: name.to_owned(),
            item: item,
            sem: sem,
            indom: indom,
            dim: dim,
            shorttext: shorthelp.to_owned(),
            longtext: longhelp.to_owned(),
            val: init_val,
            mmap_view: None
        }
    }

    pub fn val(&self) -> MetricType {
        self.val.clone()
    }

    pub fn set_val(&mut self, new_val: MetricType) {
        match self.mmap_view {
            Some(ref mut mv) => {
                let mut b_slice = unsafe { mv.as_mut_slice() };
                match (self.val, new_val) {
                    (MetricType::I64(_), MetricType::I64(new)) => {
                        b_slice.write_i64::<LittleEndian>(new).unwrap()
                    },
                    (MetricType::F64(_), MetricType::F64(new)) => {
                        b_slice.write_f64::<LittleEndian>(new).unwrap()
                    },
                    (_, _) => panic!("wrong metric type!")
                }
            },
            None => panic!("metric not yet mapped!")
        }
        self.val = new_val;
    }
}

pub struct MMV {
    path: String,
    flags: MMVFlags,
    cluster_id: u32,
}

macro_rules! write_str_with_nul {
    ($x:expr, $y:expr) => {
        $x.write($y.as_bytes()).unwrap();
        $x.write(&[0]).unwrap();
    }
}

impl MMV {
    pub fn new(path: &str, flags: MMVFlags, cluster_id: u32) -> MMV {
        MMV {
            path: path.to_owned(),
            flags: flags,
            cluster_id: cluster_id,
        }
    }

    pub fn map(&self, metrics: &mut [&mut Metric]) {
        let mut file = OpenOptions::new()
            .read(true).write(true).open(&self.path).unwrap();
        let n_metrics = metrics.len() as u64;
        let mmv_size =
            HDR_LEN + 3*TOC_BLOCK_LEN +
            n_metrics*(METRIC_BLOCK_LEN + VALUE_BLOCK_LEN + 2*STRING_BLOCK_LEN);
        for _ in 0..mmv_size {
            file.write(&[0]).unwrap();
        }

        let mut mmap = Mmap::open_with_offset(
            &file, Protection::ReadWrite, 0, mmv_size as usize).unwrap();
        self.write_mmv(&mut mmap, metrics);
        self.split_mmap_views(mmap, metrics)
    }

    fn write_mmv(&self, mmap: &mut Mmap, metrics: &[&mut Metric]) {
        let mut mmv = Cursor::new(unsafe { mmap.as_mut_slice() });
        let n_metrics = metrics.len() as u64;

        // MMV\0
        write_str_with_nul!(mmv, "MMV");
        // version
        mmv.write_u32::<LittleEndian>(1).unwrap();
        // generation1
        let gen = time::now().to_timespec().sec;
        mmv.write_i64::<LittleEndian>(gen).unwrap();
        let gen2pos = mmv.position();
        mmv.write_i64::<LittleEndian>(0).unwrap();
        // no. of toc blocks
        mmv.write_i32::<LittleEndian>(3).unwrap();
        // flags
        mmv.write_u32::<LittleEndian>(self.flags.bits()).unwrap();
        // pid
        mmv.write_i32::<LittleEndian>(getpid()).unwrap();
        // cluster id
        mmv.write_u32::<LittleEndian>(self.cluster_id).unwrap();

        // metrics TOC block
        // section type
        mmv.write_u32::<LittleEndian>(3).unwrap();
        // no. of entries
        mmv.write_u32::<LittleEndian>(n_metrics as u32).unwrap();
        // section offset
        let metric_section_offset: u64 = HDR_LEN + TOC_BLOCK_LEN*3;
        mmv.write_u64::<LittleEndian>(metric_section_offset as u64).unwrap();

        // values TOC block
        // section type
        mmv.write_u32::<LittleEndian>(4).unwrap();
        // no. of entries
        mmv.write_u32::<LittleEndian>(n_metrics as u32).unwrap();
        // section offset
        let value_section_offset = metric_section_offset + METRIC_BLOCK_LEN*n_metrics;
        mmv.write_u64::<LittleEndian>(value_section_offset).unwrap();

        // strings TOC block
        // section type
        mmv.write_u32::<LittleEndian>(5).unwrap();
        // no. of entries
        mmv.write_u32::<LittleEndian>(2*n_metrics as u32).unwrap();
        // section offset
        let string_section_offset = value_section_offset + VALUE_BLOCK_LEN*n_metrics;
        mmv.write_u64::<LittleEndian>(string_section_offset).unwrap();

        // metric, value, string blocks
        for (i, m) in metrics.iter().enumerate() {
            let i = i as u64;
            
            // metric block
            let metric_block_offset: u64 = metric_section_offset + i*METRIC_BLOCK_LEN;
            mmv.set_position(metric_block_offset);
            // name
            write_str_with_nul!(mmv, m.name);
            mmv.set_position(metric_block_offset + METRIC_NAME_MAX_LEN);
            // item
            mmv.write_u32::<LittleEndian>(m.item).unwrap();
            // type
            match m.val {
                MetricType::I64(_) => mmv.write_u32::<LittleEndian>(2).unwrap(),
                MetricType::F64(_) => mmv.write_u32::<LittleEndian>(5).unwrap(),
            }
            // sem
            mmv.write_u32::<LittleEndian>(m.sem as u32).unwrap();
            // dim
            mmv.write_u32::<LittleEndian>(m.dim).unwrap();
            // indom
            mmv.write_u32::<LittleEndian>(m.indom).unwrap();
            // zero pad
            mmv.write_u32::<LittleEndian>(0).unwrap();
            // short and long help offset
            let shorthelp_offset_offset = mmv.position();
            let longhelp_offset_offset = mmv.position() + 8;

            // value blocks
            let value_block_offset = value_section_offset + i*VALUE_BLOCK_LEN;
            mmv.set_position(value_block_offset);
            // value
            match m.val {
                MetricType::I64(x) => mmv.write_i64::<LittleEndian>(x).unwrap(),
                MetricType::F64(x) => mmv.write_u64::<LittleEndian>(unsafe {
                    transmute::<f64, u64>(x)
                }).unwrap(),
            }
            // extra
            mmv.write_u64::<LittleEndian>(0).unwrap();
            // offset to metric block
            mmv.write_u64::<LittleEndian>(metric_block_offset).unwrap();
            // offset to instance block
            mmv.write_u64::<LittleEndian>(0).unwrap();

            // string block
            let string_block_offset = string_section_offset + i*2*STRING_BLOCK_LEN;
            mmv.set_position(string_block_offset);
            // short help
            let shorthelp_offset = mmv.position();
            mmv.set_position(shorthelp_offset_offset);
            mmv.write_u64::<LittleEndian>(shorthelp_offset).unwrap();
            mmv.set_position(shorthelp_offset);
            write_str_with_nul!(mmv, m.shorttext);
            // long help
            let longhelp_offset = string_block_offset + STRING_BLOCK_LEN;
            mmv.set_position(longhelp_offset_offset);
            mmv.write_u64::<LittleEndian>(longhelp_offset).unwrap();
            mmv.set_position(longhelp_offset);
            write_str_with_nul!(mmv, m.longtext);
        }

        // unlock header
        mmv.set_position(gen2pos);
        mmv.write_i64::<LittleEndian>(gen).unwrap();
    }

    fn split_mmap_views(&self, mmap: Mmap, metrics: &mut [&mut Metric]) {
        let n_metrics = metrics.len() as u64;
        let metric_section_offset = HDR_LEN as usize + (TOC_BLOCK_LEN as usize * 3);
        let value_section_offset =
            metric_section_offset +
            METRIC_BLOCK_LEN as usize * n_metrics as usize;

        let mut right = mmap.into_view_sync();
        let mut left_mid_len = 0;
        for (i, m) in metrics.iter_mut().enumerate() {
            let value_block_offset = value_section_offset + i * VALUE_BLOCK_LEN as usize;

            let (left, r) = right.split_at(value_block_offset - left_mid_len).unwrap();
            let (middle, r) = r.split_at(8).unwrap();
            right = r;
            left_mid_len = left.len() + middle.len();

            m.mmap_view = Some(middle);
        }
    }
}