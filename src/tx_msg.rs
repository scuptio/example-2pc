use bincode::{Decode, Encode};
use scupt_util::id::XID;
use scupt_util::message::MsgTrait;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::hash::Hash;

use crate::dtm_testing_msg::DTMTesting;
use crate::tx_msg_to_tm::MsgToTM;
use crate::tx_msg_to_rm::MsgToRM;

#[derive(
Clone, Hash,
PartialEq, Eq,
Debug,
Serialize, Deserialize,
Decode, Encode
)]
pub enum TxMsg {
    TMMsg(MTxMsg<MsgToRM>),
    RMMsg(MTxMsg<MsgToTM>),
    DTMTesting(DTMTesting),
}

impl MsgTrait for TxMsg {}



#[derive(Clone, Hash, PartialEq, Eq, Debug,
Serialize, Deserialize, Decode, Encode)]
pub struct MTxMsg<T:MsgTrait + 'static> {
    pub xid: XID,
    #[serde(bound = "T: MsgTrait")]
    pub msg: T,
}

impl <T:MsgTrait + 'static> MsgTrait for MTxMsg<T> {}



