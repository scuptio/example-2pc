use std::sync::Arc;

use scupt_net::message_receiver_async::ReceiverAsync;
use scupt_net::message_sender_async::SenderAsync;
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
        auto_name:String,
        node_id: NID,
        sender: Arc<dyn SenderAsync<TxMsg>>,
        notify: Notifier,
    ) -> Self {
        let coord = TxCoordCommit::new(auto_name, node_id, sender, notify);
        Self {
            coord,
        }
    }

    pub async fn incoming(&self, receiver: Arc<dyn ReceiverAsync<TxMsg>>) -> Res<()> {
        self.coord.incoming_message(receiver).await?;
        Ok(())
    }

    pub async fn handle(&self) -> Res<()> {
        self.coord.handle().await?;
        Ok(())
    }
}

