//! Key positions, arity and the same-slot rule (DESIGN §6.8, §8.1, §23.6, §23.14).
//!
//! [`common_slot`] is used by the data node's router (cluster mode: keys in more than one slot
//! give `-CROSSSLOT Keys in request don't hash to the same slot`) and by the coordinator to
//! compute `-MOVED <slot> <primary>` for keyed commands.

use crate::spec::CommandSpec;

/// Argument indexes (into `argv`, 0 = command name) of the command's keys.
pub fn key_positions(spec: &CommandSpec, argv: &[&[u8]]) -> smallvec::SmallVec<[u16; 4]> {
    todo!("DESIGN §8.1: key positions")
}

/// Redis arity rule: `arity >= 0` exact, `arity < 0` minimum `-arity` (command name included).
pub fn check_arity(spec: &CommandSpec, argc: usize) -> bool {
    if spec.arity >= 0 {
        argc == spec.arity as usize
    } else {
        argc >= usize::from(spec.arity.unsigned_abs())
    }
}

/// The slot shared by the keys at `pos`, `None` for a keyless command, `Err(CrossSlot)` when
/// the keys map to more than one slot.
pub fn common_slot(argv: &[&[u8]], pos: &[u16]) -> Result<Option<u16>, CrossSlot> {
    todo!("DESIGN §23.14: same-slot rule")
}

/// Keys of one request map to different slots.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct CrossSlot;
