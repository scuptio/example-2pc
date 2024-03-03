#[cfg(test)]
mod tests {
    use sedeve_kit::action::action_message::ActionMessage;
    use sedeve_kit::trace_gen::action_from_state_db::read_action_message;
    use crate::test_data_path::tests::test_data_path;
    use crate::tx_msg::TxMsg;

    #[test]
    fn test_action_message() {
        let db_path = test_data_path("2pc_action.db".to_string());
        let map_path = test_data_path("2pc_map_const.json".to_string());
        let h = |_m:ActionMessage<TxMsg>| {
            Ok(())
        };
        read_action_message::<TxMsg, _>(db_path, map_path, &h).unwrap();
    }
}