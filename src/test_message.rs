#[cfg(test)]
mod tests {
    use scupt_util::id::XID;
    use scupt_util::message::{Message, MsgTrait, test_check_message};
    use scupt_util::mt_map::KeyValue;
    use crate::dtm_testing_msg::{DTMTesting, MTAccess, MTRMState, MTState, MTTMState};
    use crate::rm_state::RMState;
    use crate::tm_state::TMState;
    use crate::tx_msg::{MTxMsg, TxMsg};

    use crate::tx_msg_to_rm::{MPrepare, MsgToRM};
    use crate::tx_msg_to_tm::{MPrepareResp, MsgToTM};

    #[test]
    fn test() {
        let vec1 = vec![
            DTMTesting::Restart(1),
            DTMTesting::TxBegin(1),
            DTMTesting::RMAccess(MTAccess {
                xid: 0,
                tm_id: 0,
                rm_id: 0,
            }),
            DTMTesting::RMAbort(1),
            DTMTesting::Check(MTState {
                node_id: 0,
                rm_state: Default::default(),
                tm_state: Default::default(),
                tm_rm_collection: Default::default(),
            }),
            DTMTesting::Setup(MTState {
                node_id: 0,
                rm_state: Default::default(),
                tm_state: Default::default(),
                tm_rm_collection: Default::default(),
            }),
            DTMTesting::TMTimeout(1),
            DTMTesting::TMCommitted(1),
            DTMTesting::TMAborted(1),
            DTMTesting::TMAccess(MTAccess {
                xid: 0,
                tm_id: 0,
                rm_id: 0,
            }),
            DTMTesting::TMSendAbort(1),
            DTMTesting::TMSendCommit(1),
            DTMTesting::TMSendPrepare(1)
        ];
        test_msg_vec(vec1);
        let vec2 = vec![
            TxMsg::DTMTesting(DTMTesting::Restart(1)),
            TxMsg::RMMsg(MTxMsg::<MsgToTM> {
                xid: 1,
                msg: MsgToTM::DTMTesting(DTMTesting::Restart(1)),
            }),
            TxMsg::RMMsg(MTxMsg::<MsgToTM> {
                xid: 1,
                msg: MsgToTM::AbortedACK(0),
            }),
            TxMsg::RMMsg(MTxMsg::<MsgToTM> {
                xid: 1,
                msg: MsgToTM::CommittedACK(0),
            }),
            TxMsg::RMMsg(MTxMsg::<MsgToTM> {
                xid: 1,
                msg: MsgToTM::PrepareResp(MPrepareResp {
                    source_id: 0,
                    success: false,
                }),
            }),
            TxMsg::TMMsg(MTxMsg::<MsgToRM>{
                xid: 1,
                msg: MsgToRM::DTMTesting(DTMTesting::Restart(0)),
            }),
            TxMsg::TMMsg(MTxMsg::<MsgToRM>{
                xid: 1,
                msg: MsgToRM::Commit(1),
            }),
            TxMsg::TMMsg(MTxMsg::<MsgToRM>{
                xid: 1,
                msg: MsgToRM::Abort(1),
            }),
            TxMsg::TMMsg(MTxMsg::<MsgToRM>{
                xid: 1,
                msg: MsgToRM::Prepare(MPrepare {
                    source_id: 0,
                    rm_id: Default::default(),
                }),
            }),
        ];

        test_msg_vec(vec2);

        let vec3 = vec![
            RMState::RMRunning,
            RMState::RMAborted,
            RMState::RMPrepared,
            RMState::RMInvalid,
            RMState::RMCommitted
        ];

        let vec4 = vec![
            TMState::TMCommitted,
            TMState::TMAborted,
            TMState::TMPreparing,
            TMState::TMCommitting,
            TMState::TMAborting,
            TMState::TMInvalid,
            TMState::TMRunning
        ];

        let mut state = MTState {
            node_id: 1,
            rm_state: Default::default(),
            tm_state: Default::default(),
            tm_rm_collection: Default::default(),
        };
        for (i, s) in vec3.iter().enumerate() {
            state.rm_state.zzz_array.push(KeyValue {
                key: (i + 1) as XID,
                value: MTRMState {
                    state: s.clone(),
                    rm_id: Default::default(),
                }
            })
        }

        for (i, s) in vec4.iter().enumerate() {
            state.tm_state.zzz_array.push(KeyValue {
                key: (i + 1) as XID,
                value: MTTMState {
                    state: s.clone(),
                    rm_id: Default::default(),
                }
            })
        }

        test_msg_vec(vec![state]);
    }

    fn test_msg_vec<M: MsgTrait + 'static>(vec: Vec<M>) {
        for m in vec {
            let msg = Message::new(m, 1, 2);
            let ok = test_check_message(msg);
            assert!(ok);
        }
    }
}