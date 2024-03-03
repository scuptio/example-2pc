use bincode::{Decode, Encode};
use scupt_util::message::MsgTrait;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::hash::Hash;

#[derive(Clone, Hash, PartialEq, Eq, Debug,
Serialize, Deserialize, Decode, Encode)]
/// enum TMState definition
pub enum TMState {
    TMInvalid,
    TMRunning,
    TMPreparing,
    TMCommitting,
    TMAborting,
    TMCommitted,
    TMAborted,
} // enum TMState definition end

impl MsgTrait for TMState {}









