use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::hash::Hash;
use scupt_util::id::XID;
use scupt_util::message::MsgTrait;
use scupt_util::mt_map::MTMap;
use scupt_util::mt_set::MTSet;
use scupt_util::node_id::NID;
use crate::rm_state::RMState;
use crate::tm_state::TMState;

#[derive(
Clone,
Hash,
Debug,
PartialEq, Eq,
Serialize, Deserialize,
Decode, Encode
)]
pub enum DTMTesting {
    TMTimeout(XID),
    TMCommitted(XID),
    TMAborted(XID),
    TMSendPrepare(XID),
    TMSendCommit(XID),
    TMSendAbort(XID),
    TMAccess(MTAccess),
    RMAccess(MTAccess),
    RMAbort(XID),
    TxBegin(XID),
    Restart(NID),
    Setup(MTState),
    Check(MTState),
}

impl DTMTesting {
    pub fn tm_xid(&self) -> Option<XID> {
        match self {
            DTMTesting::TMTimeout(x) |
            DTMTesting::TMCommitted(x) |
            DTMTesting::TMAborted(x) |
            DTMTesting::TMSendPrepare(x) |
            DTMTesting::TMSendCommit(x) |
            DTMTesting::TMSendAbort(x) |
            DTMTesting::TxBegin(x)
            => {
                Some(*x)
            }
            DTMTesting::TMAccess(a) => {
                Some(a.xid)
            }
            _ => {
                None
            }
        }
    }

    pub fn rm_xid(&self) -> Option<XID> {
        match self {
            DTMTesting::RMAbort(x) => {
                Some(*x)
            }
            DTMTesting::RMAccess(a) => {
                Some(a.xid)
            }
            _ => {
                None
            }
        }
    }
}


#[derive(
Clone,
Hash,
Debug,
PartialEq, Eq,
Serialize, Deserialize,
Decode, Encode
)]
pub struct MTState {
    pub node_id :NID,
    pub rm_state : MTMap<XID, MTRMState>,
    pub tm_state : MTMap<XID, MTTMState>,
    pub tm_rm_collection: MTMap<XID, MTMap<NID, RMState>>
}

#[derive(
Clone,
Hash,
Debug,
PartialEq, Eq,
Serialize, Deserialize,
Decode, Encode
)]
pub struct MTTMState {
    pub state: TMState,
    pub rm_id:MTSet<NID>
}


#[derive(
Clone,
Hash,
Debug,
PartialEq, Eq,
Serialize, Deserialize,
Decode, Encode
)]
pub struct MTRMState {
    pub state: RMState,
    pub rm_id:MTSet<NID>
}


#[derive(
Clone,
Hash,
Debug,
PartialEq, Eq,
Serialize, Deserialize,
Decode, Encode
)]
pub struct MTAccess {
    pub xid: XID,
    pub tm_id : NID,
    pub rm_id : NID,
}
impl MsgTrait for MTRMState {

}

impl MsgTrait for MTTMState {

}

impl MsgTrait for MTState {

}

impl MsgTrait for MTAccess {

}

impl MsgTrait for DTMTesting {

}