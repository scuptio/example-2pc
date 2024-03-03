use scupt_net::message_sender::Sender;
use scupt_net::opt_send::OptSend;
use scupt_net::notifier::Notifier;
use scupt_util::error_type::ET;
use scupt_util::id::XID;
use scupt_util::message::Message;
use scupt_util::node_id::NID;
use scupt_util::res::Res;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use scupt_net::task::spawn_local_task;
use scupt_util::mt_set::MTSet;
use scupt_util::res_of::res_option;
use sedeve_kit::{auto_enable, internal_begin, internal_end, output};
use tokio::select;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::Mutex;
use sedeve_kit::player::automata;
use crate::name::TX_COORD_COMMIT;
use tracing::{debug, debug_span, Instrument, trace};
use crate::dtm_testing_msg::{DTMTesting, MTAccess, MTState};


use crate::rm_state::RMState;
use crate::tm_state::TMState;

use crate::tx_coord_event::TxCoordEvent;
use crate::tx_msg::{MTxMsg, TxMsg};

use crate::tx_msg_to_tm::{MPrepareResp, MsgToTM};
use crate::tx_msg_to_rm::{MPrepare, MsgToRM};

pub struct TxTM {
    _xid: XID,
    notify: Notifier,
    sender: UnboundedSender<Option<MsgToTM>>,
    receiver: Mutex<UnboundedReceiver<Option<MsgToTM>>>,
    inner: Mutex<_TxTM>,
}

struct _TxTM {
    channel: Arc<dyn Sender<TxMsg>>,
    node_id: NID,
    xid: XID,
    state: TMState,
    num_committed: u32,
    num_aborted: u32,
    num_prepared: u32,

    rm_id:HashSet<NID>,
    rm_state: HashMap<NID, RMState>,
    sender: UnboundedSender<TxCoordEvent>,
}

impl TxTM {
    pub fn new(
        node_id: NID, xid: XID,
        state: TMState,
        rm_ids: Vec<NID>,
        channel: Arc<dyn Sender<TxMsg>>,
        sender: UnboundedSender<TxCoordEvent>,
        stop_notifier: Notifier,
    ) -> Arc<Self> {
        let (s, r) = unbounded_channel();
        let inner = Mutex::new(_TxTM::new(node_id, xid, state, rm_ids, channel, sender));
        let name = format!("tm_{}", xid);
        let notify = Notifier::new_with_name(name.clone());
        let tm = Self {
            _xid: xid,
            notify,
            sender: s,
            receiver: Mutex::new(r),
            inner,
        };
        let tm_p1 = Arc::new(tm);
        let tm_p2 = tm_p1.clone();
        trace!("task {} begin", name);
        let _ = spawn_local_task(stop_notifier.clone(), name.as_str(), async move {
            tm_p2.event_loop().await;
        });
        tm_p1
    }



    pub fn close(&self) {
        let _ = self.sender.send(None);
    }

    async fn event_loop(&self) {
        let f = self.tm_event_loop_inner();
        let _ = select! {
            _ = self.notify.notified() => {

            }
            _r = f => {

            }
         };
    }

    async fn tm_event_loop_inner(&self) -> Res<()> {
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
                inner.recv_msg(event).instrument(debug_span!("TM recv_msg")).await?;
            } else {
                break;
            }

        }
        Ok(())
    }

    pub async fn recv_msg(&self, message: MsgToTM, _source: NID, _dest: NID) -> Res<()> {
        self.send_to_tm(message).await?;
        Ok(())
    }


    async fn send_to_tm(&self, event: MsgToTM) -> Res<()> {
        let r = self.sender.send(Some(event));
        match r {
            Ok(()) => { Ok(()) }
            Err(_) => { Err(ET::TokioSenderError("send tm event error".to_string())) }
        }
    }

    pub async fn check_tm(&self, state:MTState) -> Res<()> {
        let guard = self.inner.lock().await;
        guard.check(state)?;
        Ok(())
    }

    pub async fn setup_tm(&self, state:MTState) -> Res<()>{
        let mut guard = self.inner.lock().await;
        guard.setup(state)?;
        Ok(())
    }

    pub async fn restart(&self) -> Res<()> {
        let mut guard = self.inner.lock().await;
        guard.restart()?;
        Ok(())
    }
}

impl _TxTM {
    fn new(node_id: NID, xid: XID,
           state: TMState,
           rm_ids: Vec<NID>,
           channel: Arc<dyn Sender<TxMsg>>,
           sender: UnboundedSender<TxCoordEvent>,
    ) -> Self {
        let mut rm_state = HashMap::new();
        for id in rm_ids {
            rm_state.insert(id, RMState::RMRunning);
        }
        Self {
            channel,
            node_id,
            xid,
            state,
            num_committed: 0,
            num_aborted: 0,
            num_prepared: 0,
            rm_id: Default::default(),
            rm_state,
            sender,
        }
    }

    async fn recv_msg(&mut self, event: MsgToTM) -> Res<()> {
        debug!("enter nid:{} xid:{} incoming tm event {:?}", self.node_id, self.xid, event);
        let e = event.clone();
        match event {
            MsgToTM::PrepareResp(prepare) => {
                self.recv_prepare_resp(prepare).await?;
            }
            MsgToTM::CommittedACK(source) => {
                self.recv_committed_ack(source).await?;
            }
            MsgToTM::AbortedACK(source) => {
                self.recv_aborted_ack(source).await?;
            }
            MsgToTM::DTMTesting(m) => {
                self.handle_dtm(m).await?;
            }
        }
        debug!("exit nid:{} xid:{} incoming tm event {:?}", self.node_id, self.xid, e);
        Ok(())
    }

    async fn handle_dtm(&mut self, m: DTMTesting) -> Res<()> {
        match m {
            DTMTesting::TMTimeout(_) => {
                self.timeout().await?;
            }
            DTMTesting::TMSendPrepare(_) => {
                self.tm_send_prepare().await?;
            }
            DTMTesting::TMAccess(a) => {
                self.tx_access(a)?;
            }
            DTMTesting::TxBegin(_) => {
                self.tx_begin()?;
            }
            _ => {}
        }
        Ok(())
    }
    async fn recv_prepare_resp(&mut self, prepare:MPrepareResp) -> Res<()> {
        if prepare.success {
            self.recv_prepared_ack(prepare.source_id).await?;
        } else {
            self.recv_prepare_abort(prepare.source_id).await?;
        }
        Ok(())
    }

    async fn recv_prepared_ack(&mut self, source: NID) -> Res<()> {
        if self.state == TMState::TMPreparing {
            self.recv_prepared_ack_inner(source).await?;
        }
        Ok(())
    }

    async fn recv_prepared_ack_inner(&mut self, source: NID) -> Res<()> {
        debug!("enter tx {} tm recv prepared ACK from {}", self.xid, source);
        let opt = self.rm_state.get_mut(&source);
        match opt {
            Some(t) => {
                if *t == RMState::RMRunning {
                    *t = RMState::RMPrepared;
                    self.num_prepared += 1;
                }
            }
            None => {}
        }
        self.tm_advance().await?;
        debug!("exit tx {} tm recv prepared ACK from {}", self.xid, source);
        Ok(())
    }

    async fn recv_prepare_abort(&mut self, source: NID) -> Res<()> {
        if self.state == TMState::TMPreparing {
            self.recv_prepare_abort_inner(source).await?;
        }
        Ok(())
    }

    async fn recv_prepare_abort_inner(&mut self, source: NID) -> Res<()> {
        debug!("{} recv prepare abort", self.xid);
        if self.state == TMState::TMPreparing {
            let opt = self.rm_state.get_mut(&source);
            match opt {
                Some(t) => {
                    if *t == RMState::RMRunning {
                        *t = RMState::RMAborted;
                        self.num_aborted += 1;
                    }
                }
                None => {}
            }
            self.tm_advance()
                .instrument(debug_span!("tm_advance")).await?;
        }
        debug!("{} recv prepare abort done", self.xid);
        Ok(())
    }

    async fn tm_advance(&mut self) -> Res<()> {
        if self.num_aborted > 0 && self.num_aborted + self.num_prepared == self.rm_state.len() as u32 {
            self.tm_send_abort().instrument(debug_span!("tm_send_abort")).await?;
        } else if self.num_prepared == self.rm_state.len() as u32 {
            self.tm_send_commit().instrument(debug_span!("tm_send_commit")).await?;
        }
        Ok(())
    }

    async fn tm_send_commit(&mut self) -> Res<()> {
        let _m = Message::new(TxMsg::DTMTesting(DTMTesting::TMSendCommit(self.xid)), self.node_id, self.node_id);
        internal_begin!(TX_COORD_COMMIT, _m.clone());
        debug!("{} tm send commit", self.xid);
        // PREPARING, COMMITTING
        if self.state == TMState::TMPreparing {
            self.state = TMState::TMCommitting;
        }
        internal_end!(TX_COORD_COMMIT, _m.clone());
        if self.state == TMState::TMCommitting {
            self.tm_commit().await?;
        }
        debug!("{} tm send commit end", self.xid);

        Ok(())
    }

    async fn tm_send_abort(&mut self) -> Res<()> {
        let _m = Message::new(TxMsg::DTMTesting(DTMTesting::TMSendAbort(self.xid)), self.node_id, self.node_id);
        internal_begin!(TX_COORD_COMMIT, _m.clone());
        debug!("{} tm send abort", self.xid);
        // RUNNING, PREPARING, ABORTING
        if self.state == TMState::TMPreparing ||
            self.state == TMState::TMRunning {
            self.state = TMState::TMAborting;
        }
        internal_end!(TX_COORD_COMMIT, _m.clone());
        if self.state == TMState::TMAborting {
            self.tm_abort().await?;
        }

        debug!("{} tm send abort end", self.xid);
        Ok(())
    }

    async fn recv_committed_ack(&mut self, source: NID) -> Res<()> {
        if self.state == TMState::TMCommitting {
            self.recv_committed_ack_inner(source).await?;
        }
        Ok(())
    }

    async fn recv_committed_ack_inner(&mut self, source: NID) -> Res<()> {
        debug!("enter tm {} recv committed ACK from {}", self.xid, source);
        if self.state == TMState::TMCommitting {
            let opt = self.rm_state.get_mut(&source);
            match opt {
                Some(t) => {
                    if *t == RMState::RMPrepared || *t == RMState::RMRunning {
                        *t = RMState::RMCommitted;
                        self.num_committed += 1;
                    }
                }
                None => {}
            }
            if self.num_committed == self.rm_state.len() as u32 {
                self.tm_committed().await?;
            }
        }
        debug!("exit tm {} recv committed ACK from {}", self.xid, source);
        Ok(())
    }

    async fn recv_aborted_ack(&mut self, source: NID) -> Res<()> {
        if self.state == TMState::TMAborting {
            debug!("tm aborted_ack rm state: {:?}", self.rm_state);
            self.recv_aborted_ack_inner(source).await?
        } else {
            debug!("tm aborted_ack tm state: {:?}", self.state);
        }
        Ok(())
    }

    async fn recv_aborted_ack_inner(&mut self, source: NID) -> Res<()> {
        debug!("enter recv_aborted_ack_inner {}", self.xid);
        let opt = self.rm_state.get_mut(&source);
        match opt {
            Some(t) => {
                if *t == RMState::RMRunning || *t == RMState::RMPrepared {
                    *t = RMState::RMAborted;
                    self.num_aborted += 1;
                }
            }
            None => {
                panic!("error")
            }
        }

        if self.num_aborted == self.rm_state.len() as u32 {
            self.tm_aborted().await?;
        }

        debug!("exit recv_aborted_ack_inner {}" , self.xid);
        Ok(())
    }

    async fn tm_committed(&mut self) -> Res<()> {
        if self.state == TMState::TMCommitting {
            self.tm_committed_inner().await?;
        }
        Ok(())
    }

    async fn tm_aborted(&mut self) -> Res<()> {
        if self.state == TMState::TMAborting {
            self.tm_aborted_inner().await?;
        }
        Ok(())
    }

    async fn tm_committed_inner(&mut self) -> Res<()> {
        let _m = Message::new(TxMsg::DTMTesting(DTMTesting::TMCommitted(self.xid)), self.node_id, self.node_id);
        internal_begin!(TX_COORD_COMMIT, _m.clone());
        if self.state == TMState::TMCommitting {
            self.state = TMState::TMCommitted;
            self.end()?;
        }
        internal_end!(TX_COORD_COMMIT, _m.clone());
        Ok(())
    }

    fn end(&self) -> Res<()>  {
        let _ = self.sender.send(TxCoordEvent::TMEnd(self.xid));
        Ok(())
    }

    async fn tm_aborted_inner(&mut self) -> Res<()> {
        let _m = Message::new(TxMsg::DTMTesting(DTMTesting::TMAborted(self.xid)), self.node_id, self.node_id);
        internal_begin!(TX_COORD_COMMIT, _m.clone());
        if self.state == TMState::TMAborting {
            self.state = TMState::TMAborted;
            self.end()?;
        }
        internal_end!(TX_COORD_COMMIT, _m.clone());
        Ok(())
    }

    async fn tm_send_prepare(&mut self) -> Res<()> {
        if self.state == TMState::TMRunning {
            self.state = TMState::TMPreparing;
        } else {
            return Ok(());
        }
        if self.state == TMState::TMPreparing {
            self.tm_prepare()
                .instrument(debug_span!("tm_prepare"))
                .await?;
        }
        Ok(())
    }

    async fn tm_prepare(&mut self) -> Res<()> {
        let rm_ids:Vec<NID> = self.rm_id.iter().map(|n|{*n}).collect();
        let m = if rm_ids.len() == 1 {
            self.state = TMState::TMInvalid;
            MsgToRM::Commit(self.node_id)
        } else {
            MsgToRM::Prepare(MPrepare {
                source_id: self.node_id,
                rm_id: MTSet {
                    zzz_array: rm_ids,
                },
            })
        };
        for (nid, rm_state) in self.rm_state.iter() {
            if *rm_state == RMState::RMRunning {
                self.send_to_rm(m.clone(), *nid).await?;
            }
        }
        Ok(())
    }

    async fn tm_abort(&self) -> Res<()> {
        debug!("enter {} tm abort", self.xid);
        debug!("tm abort rm state: {:?}", self.rm_state);
        for (nid, rm_state) in self.rm_state.iter() {
            assert_ne!(*rm_state, RMState::RMCommitted);
            if *rm_state == RMState::RMRunning ||
                *rm_state == RMState::RMPrepared {
                let m = MsgToRM::Abort(self.node_id);
                self.send_to_rm(m, *nid).await?;
            }
        }
        debug!("exit {} tm abort", self.xid);
        Ok(())
    }

    async fn tm_commit(&mut self) -> Res<()> {
        debug!("enter {} tm commit", self.xid);
        for (nid, rm_state) in self.rm_state.iter() {
            assert_ne!(*rm_state, RMState::RMAborted);
            if *rm_state == RMState::RMPrepared {
                self.send_to_rm(MsgToRM::Commit(self.node_id), *nid).await?;
            }
        }

        debug!("exit {} tm commit", self.xid);
        Ok(())
    }

    async fn timeout(&mut self) -> Res<()> {
        debug!("enter timeout xid:{}", self.xid);
        match self.state {
            TMState::TMRunning | TMState::TMPreparing | TMState::TMAborting => {
                self.state = TMState::TMAborting;
            }
            _ => {}
        }

        match self.state {
            TMState::TMCommitting => {
                self.tm_send_commit()
                    .instrument(debug_span!("tm_commit"))
                    .await?;
            }
            TMState::TMAborting => {
                self.tm_send_abort()
                    .instrument(debug_span!("tm_abort"))
                    .await?;
            }
            _ => {}
        }
        debug!("exit timeout xid:{}", self.xid);
        Ok(())
    }


    async fn send_to_rm(&self, msg: MsgToRM, dest: NID) -> Res<()> {
        let m = Message::new(TxMsg::TMMsg(
            MTxMsg {
                xid: self.xid,
                msg,
            }
        ), self.node_id, dest);

        let _m = m.clone();
        output!(TX_COORD_COMMIT, _m);
        if auto_enable!(TX_COORD_COMMIT) {
            return Ok(())
        }

        self.channel.send(m, OptSend::default()).await
    }

    fn check(&self, m:MTState) -> Res<()> {
        let _m = m.tm_state.to_map();
        let opt_tm_state = _m.get(&self.xid);
        assert!(opt_tm_state.is_some());
        let tm_state = res_option(opt_tm_state)?;
        if self.state == TMState::TMInvalid {
            assert!(tm_state.state == TMState::TMCommitted ||
                tm_state.state == TMState::TMAborted ||
                tm_state.state == TMState::TMInvalid)
        } else {
            assert_eq!(self.state, tm_state.state);
            assert_eq!(self.rm_id, tm_state.rm_id.to_set());
            let mut map1 = HashMap::new();
            for (n, s) in self.rm_state.iter() {
                if *s != RMState::RMInvalid {
                    map1.insert(*n, s.clone());
                }
            }
            let _m = m.tm_rm_collection.to_map();
            let opt_tm_rm_collection = _m.get(&self.xid);
            assert!(opt_tm_rm_collection.is_some());
            let tm_rm_collection = res_option(opt_tm_rm_collection)?;
            let map2 = tm_rm_collection.to_map();
            for (n, s) in map2 {
                let opt = map1.remove(&n);
                if let Some(_rm_s) = opt {
                    assert_eq!(s, _rm_s);
                } else {
                    assert_eq!(s, RMState::RMInvalid);
                }
            }
            assert!(map1.is_empty());
        }
        Ok(())
    }

    fn setup(&mut self, m:MTState) -> Res<()> {
        let _m = m.tm_state.to_map();
        let opt_tm_state = _m.get(&self.xid);
        let tm_state = res_option(opt_tm_state)?;
        let _m = m.tm_rm_collection.to_map();
        let opt_tm_rm_collection = _m.get(&self.xid);
        let tm_rm_collection = res_option(opt_tm_rm_collection)?;
        self.state = tm_state.state.clone();

        self.rm_id = tm_state.rm_id.to_set();
        self.rm_state.clear();
        for (n, s) in tm_rm_collection.to_map() {
            if s != RMState::RMInvalid {
                self.rm_state.insert(n, s);
            }
        }
        assert_eq!(self.rm_id, self.rm_state.keys().map(|x|{*x}).collect());

        Ok(())
    }

    fn tx_begin(&mut self) -> Res<()> {
        self.state = TMState::TMRunning;
        Ok(())
    }

    fn tx_access(&mut self, m :MTAccess) -> Res<()> {
        assert_eq!(self.node_id, m.tm_id);
        assert_eq!(self.xid, m.xid);
        if self.state == TMState::TMRunning {
            self.rm_id.insert(m.rm_id);
            self.rm_state.insert(m.rm_id, RMState::RMRunning);
        }
        Ok(())
    }

    fn restart(&mut self) -> Res<()> {
        let state = self.state.clone();
        let ids = self.rm_id.clone();
        match state {
            TMState::TMAborting | TMState::TMPreparing => {
                self.state = TMState::TMAborting;
            }
            TMState::TMRunning => {
                self.state = TMState::TMInvalid;
                self.rm_id.clear();
                self.end()?;
            }
            _ => {}
        }

        self.num_aborted = 0;
        self.num_prepared = 0;
        self.num_committed = 0;
        match state {
            TMState::TMAborting | TMState::TMPreparing | TMState::TMCommitting => {
                self.rm_state.clear();
                for id in ids {
                    self.rm_state.insert(id, RMState::RMRunning);
                }
            }
            _ => {}
        }
        Ok(())
    }
}
