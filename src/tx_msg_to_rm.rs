use bincode::{Decode, Encode};

use scupt_util::message::MsgTrait;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::hash::Hash;
use scupt_util::mt_set::MTSet;
use scupt_util::node_id::NID;
use crate::dtm_testing_msg::DTMTesting;

#[derive(Clone, Hash, PartialEq, Eq, Debug,
Serialize, Deserialize, Decode, Encode)]
pub struct MPrepare {
    pub source_id:NID,
    pub rm_id:MTSet<NID>
}

impl MsgTrait for MPrepare {

}

#[derive(Clone, Hash, PartialEq, Eq, Debug,
Serialize, Deserialize, Decode, Encode)]
pub enum MsgToRM {
    Prepare(MPrepare),
    Commit(NID),
    Abort(NID),
    DTMTesting(DTMTesting),
}

impl MsgTrait for MsgToRM {}