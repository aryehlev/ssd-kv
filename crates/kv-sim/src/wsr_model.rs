//! Stateright model of whole-state replication (DESIGN §23.3, §25.3).
//!
//! 3 and 5 members, message loss and reordering, crash/restart with the A/B files, and
//! single-server SETPEERS changes applied to members in any order. Properties: at most one value
//! committed per version; committed state never lost; leader completeness; at most one leader per
//! term across peer generations.

use stateright::Model;

/// Model parameters.
#[derive(Clone, Copy, Debug)]
pub struct WsrModel {
    pub members: u8,
    pub max_term: u64,
    pub max_version: u64,
}

/// Global state of the modelled group.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct WsrState {
    pub terms: Vec<u64>,
    pub committed: Vec<u64>,
}

/// One modelled step.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum WsrAction {
    Timeout(u8),
    Deliver(u32),
    Drop(u32),
    Crash(u8),
    SetPeers(u8),
}

impl Model for WsrModel {
    type State = WsrState;
    type Action = WsrAction;

    fn init_states(&self) -> Vec<Self::State> {
        todo!("DESIGN §25.3: WSR model")
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        todo!("DESIGN §25.3: WSR model")
    }

    fn next_state(&self, last_state: &Self::State, action: Self::Action) -> Option<Self::State> {
        todo!("DESIGN §25.3: WSR model")
    }
}
