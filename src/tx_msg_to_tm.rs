

use bincode::{Decode, Encode};
use scupt_util::message::MsgTrait;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::hash::Hash;
use scupt_util::node_id::NID;
use crate::dtm_testing_msg::DTMTesting;


#[derive(Clone, Hash, PartialEq, Eq, Debug,
Serialize, Deserialize, Decode, Encode)]
pub enum MsgToTM {
    PrepareResp(MPrepareResp),
    CommittedACK(NID),
    AbortedACK(NID),
    DTMTesting(DTMTesting),
}

impl MsgTrait for MsgToTM {

}


#[derive(Clone, Hash, PartialEq, Eq, Debug,
Serialize, Deserialize, Decode, Encode)]
pub struct MPrepareResp {
    pub source_id:NID,
    pub success:bool,
}

impl MsgTrait for MPrepareResp {}