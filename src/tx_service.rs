use std::sync::Arc;
use scupt_net::message_receiver::Receiver;
use scupt_net::message_sender::Sender;
use scupt_net::notifier::Notifier;
use scupt_util::node_id::NID;
use scupt_util::res::Res;
use crate::tx_coord_commit::TxCoordCommit;
use crate::tx_msg::TxMsg;

pub struct TxService {
    coord: TxCoordCommit,
}

impl TxService {
    pub fn new(
        node_id: NID,
        sender: Arc<dyn Sender<TxMsg>>,
        notify: Notifier,
    ) -> Self {
        let coord = TxCoordCommit::new(node_id, sender, notify);
        Self {
            coord,
        }
    }

    pub async fn incoming(&self, receiver:Arc<dyn Receiver<TxMsg>>) -> Res<()> {
        self.coord.incoming_message(receiver).await?;
        Ok(())
    }

    pub async fn handle(&self) -> Res<()> {
        self.coord.handle().await?;
        Ok(())
    }
}

