use scupt_util::id::XID;

pub enum TxCoordEvent {
    TMEnd(XID),
    RMEnd(XID),
}
