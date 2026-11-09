//! LC-23 `backlog_never_reaches_disk`: the replication ring lives in memory that is locked
//! (or backed by huge pages, never swapped) and excluded from core dumps, as the kernel reports
//! it for the mapping that holds the ring (`/proc/self/smaps` `VmFlags`). A backlog has no file
//! or descriptor (compile-time assertion in `backlog.rs`).

use kv_repl::backlog::Backlog;
use kv_repl::codec::encode_group;
use kv_repl::{Group, Op, Seq};

#[test]
fn backlog_never_reaches_disk() {
    let mut b = Backlog::new(3 << 20);
    let mut g = Vec::new();
    encode_group(
        &Group {
            seq: Seq::new(1, 1),
            ops: vec![Op::Put {
                db: 0,
                vtype: 1,
                expire_at: 0,
                key: b"k".as_slice().into(),
                value: vec![7; 100],
            }],
        },
        &mut g,
    );
    b.push(Seq::new(1, 1), &g);
    let addr = b.ring_addr();
    let smaps = std::fs::read_to_string("/proc/self/smaps").expect("smaps");
    let mut flags: Option<&str> = None;
    let mut inside = false;
    for line in smaps.lines() {
        let head = line.split_whitespace().next().unwrap_or("");
        if let Some((lo, hi)) = head.split_once('-')
            && let (Ok(lo), Ok(hi)) = (usize::from_str_radix(lo, 16), usize::from_str_radix(hi, 16))
        {
            inside = lo <= addr && addr < hi;
            continue;
        }
        if inside && let Some(f) = line.strip_prefix("VmFlags:") {
            flags = Some(f);
            break;
        }
    }
    let flags: Vec<&str> = flags
        .expect("the ring's mapping is listed")
        .split_whitespace()
        .collect();
    assert!(
        flags.contains(&"dd"),
        "not excluded from core dumps: {flags:?}"
    );
    assert!(
        flags.contains(&"lo") || flags.contains(&"ht"),
        "neither locked nor huge pages: {flags:?}"
    );
    assert_eq!(b.is_huge(), flags.contains(&"ht"));
}
