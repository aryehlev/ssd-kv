//! CLUSTER and related commands on a data node (DESIGN §8.7, LC-24).
//!
//! Standalone: every CLUSTER command, READONLY, READWRITE and ASKING reply
//! `-ERR This instance has cluster support disabled`. Cluster: CLUSTER KEYSLOT, CLUSTER MYID (own
//! id only), CLUSTER INFO with constant fields (`cluster_state` from the lease, 0 for every slot,
//! epoch and node-count field); topology requests reply
//! `-ERR cluster topology is served by the coordinator`; membership and slot management replies
//! `-ERR cluster membership and slots are managed by kvadm`; ASKING is a no-op `+OK`; READONLY /
//! READWRITE set the connection flag. A data node never names another data node.
