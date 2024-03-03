
use bincode::{Decode, Encode};
use scupt_util::message::MsgTrait;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::hash::Hash;

#[derive(Clone, Hash,
PartialEq, Eq, Debug,
Serialize, Deserialize, Decode, Encode)]
/// enum RMState definition
pub enum RMState {
    RMInvalid,
    RMRunning,
    RMPrepared,
    RMCommitted,
    RMAborted,
} // enum RMState definition end

impl MsgTrait for RMState {}








