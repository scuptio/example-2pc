use std::collections::HashSet;
use scupt_net::message_sender::Sender;
use scupt_net::opt_send::OptSend;
use scupt_net::notifier::Notifier;
use scupt_util::error_type::ET;
use scupt_util::id::XID;
use scupt_util::message::Message;
use scupt_util::node_id::NID;
use scupt_util::res::Res;
use std::sync::Arc;
use scupt_net::task::spawn_local_task;
use scupt_util::res_of::res_option;
use sedeve_kit::player::automata;
use sedeve_kit::{auto_enable, output};
use tokio::select;
use tokio::sync::mpsc::{
    unbounded_channel,
    UnboundedReceiver,
    UnboundedSender,
};
use tokio::sync::Mutex;
use crate::dtm_testing_msg::{DTMTesting, MTAccess, MTState};
use crate::name::TX_COORD_COMMIT;
use crate::rm_state::RMState;
use crate::tx_coord_event::TxCoordEvent;
use crate::tx_msg::{MTxMsg, TxMsg};

use crate::tx_msg_to_tm::{MPrepareResp, MsgToTM};
use crate::tx_msg_to_rm::{MPrepare, MsgToRM};


pub struct TxRM {
    _xid: XID,
    notify: Notifier,
    sender: UnboundedSender<Option<MsgToRM>>,
    receiver: Mutex<UnboundedReceiver<Option<MsgToRM>>>,
    inner: Mutex<_TxRM>,
}

struct _TxRM {
    node_id: NID,
    xid: XID,
    state: RMState,
    rm_id:HashSet<NID>,
    channel: Arc<dyn Sender<TxMsg>>,
    sender: UnboundedSender<TxCoordEvent>,
}

impl TxRM {
    pub fn new(
        nid: NID,
        xid: XID,
        state: RMState,
        channel: Arc<dyn Sender<TxMsg>>,
        sender: UnboundedSender<TxCoordEvent>,
        stop_notifier: Notifier,
    ) -> Arc<Self> {
        let (s, r) = unbounded_channel();
        let notify = Notifier::new();
        let rm = Self {
            _xid: xid,
            notify,
            sender: s,
            receiver: Mutex::new(r),
            inner: Mutex::new(_TxRM::new(nid, xid, state, channel, sender)),
        };
        let name = format!("rm_{}", xid);
        let rm_p1 = Arc::new(rm);
        let rm_p2 = rm_p1.clone();
        let _ = spawn_local_task(stop_notifier.clone(), name.as_str(), async move {
            rm_p2.event_loop().await;
        });
        rm_p1
    }

    pub async fn check_rm(&self, state:MTState) -> Res<()> {
        let guard = self.inner.lock().await;
        guard.check(state)?;
        Ok(())
    }

    pub async fn setup_rm(&self, state:MTState) -> Res<()>{
        let mut guard = self.inner.lock().await;
        guard.setup(state)?;
        Ok(())
    }

    pub fn close(&self) {
        let _ = self.sender.send(None);
    }

    pub async fn restart(&self) -> Res<()> {
        let mut guard = self.inner.lock().await;
        guard.restart().await?;
        Ok(())
    }
    async fn event_loop(&self) {
        let f = self.rm_event_loop_inner();
        let _ = select! {
            _ = self.notify.notified() => {

            }
            _r = f => {

            }
         };
    }

    async fn rm_event_loop_inner(&self) -> Res<()> {
        let mut receiver = self.receiver.lock().await;
        loop {
            let opt = receiver.recv().await;
            if let Some(_opt) = opt {
                let event = match _opt {
                    Some(event) => { event }
                    None => { break; }
                };
                if self.notify.is_notified() {
                    break;
                }
                let mut inner = self.inner.lock().await;
                inner.recv_msg(event).await?;
            } else {
                break;
            }
        }
        Ok(())
    }

    pub async fn recv_msg(&self, message: MsgToRM, _source: NID, _dest: NID) -> Res<()> {
        self.send_rm_event(message).await?;
        Ok(())
    }

    async fn send_rm_event(&self, event: MsgToRM) -> Res<()> {
        let r = self.sender.send(Some(event));
        match r {
            Ok(()) => { Ok(()) }
            Err(_) => { Err(ET::TokioSenderError("send tm event error".to_string())) }
        }
    }
}

impl _TxRM {
    fn new(node_id: NID, xid: XID,
           state: RMState,
           channel: Arc<dyn Sender<TxMsg>>,
           sender: UnboundedSender<TxCoordEvent>,
    ) -> Self {
        Self {
            node_id,
            xid,
            state,
            rm_id: Default::default(),
            channel,
            sender,
        }
    }


    async fn recv_msg(&mut self, event: MsgToRM) -> Res<()> {
        match event {
            MsgToRM::Prepare(m) => {
                self.handle_prepare(m).await?;
            }
            MsgToRM::Commit(source) => {
                self.handle_commit(source).await?;
            }
            MsgToRM::Abort(source) => {
                self.handle_abort(source).await?;
            }
            MsgToRM::DTMTesting(m) => {
                self.handle_dtm(m).await?;
            }
        }
        Ok(())
    }

    async fn handle_dtm(&mut self, m: DTMTesting) -> Res<()> {
        match m {
            DTMTesting::RMAccess(m) => {
                self.access(m)?;
            }
            DTMTesting::RMAbort(_r) => {
                self.rm_abort().await?;
            }
            _ => {}
        }
        Ok(())
    }
    async fn handle_prepare(&mut self, prepare:MPrepare) -> Res<()> {
        let tm_node = prepare.source_id;
        if self.state == RMState::RMRunning ||
            self.state == RMState::RMPrepared ||
            self.state == RMState::RMCommitted {
            if self.state == RMState::RMRunning {
                self.state = RMState::RMPrepared;
                self.rm_id = prepare.rm_id.to_set();
            }
            self.send_prepare_resp(tm_node, true).await?;
        } else if self.state == RMState::RMAborted || self.state == RMState::RMInvalid {
            self.send_prepare_resp(tm_node, false).await?;
        } else {
            self.send_prepare_resp(tm_node, false).await?;
        }
        Ok(())
    }

    async fn handle_commit(&mut self, tm_node: NID) -> Res<()> {
        if self.state == RMState::RMPrepared {
            self.state = RMState::RMCommitted;
        }
        assert_ne!(self.state, RMState::RMAborted);
        self.send_ack(tm_node, true).await?;
        self.end().await?;
        Ok(())
    }

    async fn handle_abort(&mut self, tm_node: NID) -> Res<()> {
        self.rm_abort().await?;
        self.send_ack(tm_node, false).await?;
        Ok(())
    }

    async fn rm_abort(&mut self) -> Res<()> {
        if self.state == RMState::RMPrepared || self.state == RMState::RMRunning {
            self.state = RMState::RMAborted;
        }
        self.end().await?;
        Ok(())
    }

    async fn send_to_tm(&self, msg: MsgToTM, source: NID, dest: NID) -> Res<()> {
        let m = Message::new(
            TxMsg::RMMsg(MTxMsg {
                    xid: self.xid,
                    msg,
                }),
            source,
            dest);

        let _m = m.clone();
        output!(TX_COORD_COMMIT, _m);
        if auto_enable!(TX_COORD_COMMIT) {
           return Ok(())
        }

        self.channel.send(m, OptSend::default()).await
    }

    async fn send_prepare_resp(&self, tm_node: NID, success:bool) -> Res<()> {
        self.send_to_tm(
            MsgToTM::PrepareResp(MPrepareResp {
                source_id: self.node_id,
                success,
            }),
            self.node_id,
            tm_node,
        ).await
    }
    
    async fn send_ack(&self, tm_node: NID, commit:bool) -> Res<()> {
        let msg = if commit {
            MsgToTM::CommittedACK(self.node_id)
        } else {
            MsgToTM::AbortedACK(self.node_id)
        };
        self.send_to_tm(
             msg,
            self.node_id,
            tm_node,
        ).await
    }

    async fn end(&self) -> Res<()> {
        let r = self.sender.send(TxCoordEvent::RMEnd(self.xid));
        match r {
            Ok(()) => {}
            Err(_e) => {}
        }
        Ok(())
    }

    fn check(&self, m:MTState) -> Res<()> {
        let _m = m.rm_state.to_map();
        let opt_tm_state = _m.get(&self.xid);
        let rm_state = res_option(opt_tm_state)?;
        if self.state == RMState::RMInvalid {
            assert!(self.state == RMState::RMInvalid ||
                self.state == RMState::RMAborted ||
                self.state == RMState::RMCommitted);
        } else {
            assert_eq!(self.state, rm_state.state);
            assert_eq!(self.rm_id, rm_state.rm_id.to_set());
        }
        Ok(())
    }

    fn setup(&mut self, m:MTState) -> Res<()> {
        let _m = m.rm_state.to_map();
        let opt_tm_state = _m.get(&self.xid);
        let rm_state = res_option(opt_tm_state)?;
        self.state = rm_state.state.clone();
        self.rm_id = rm_state.rm_id.to_set();
        Ok(())
    }

    fn access(&mut self, _m:MTAccess) -> Res<()> {
        if self.state == RMState::RMInvalid {
            self.state = RMState::RMRunning;
        }
        Ok(())
    }

    async fn restart(&mut self) -> Res<()> {
        match self.state {
            RMState::RMRunning => {
                self.state = RMState::RMAborted;
                self.rm_id.clear();
                self.end().await?
            }
            _ => {}
        }
        Ok(())
    }
}
