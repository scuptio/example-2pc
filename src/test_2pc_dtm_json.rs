#[cfg(test)]
mod tests {
    use crate::test_2pc_dtm::tests::test_2pc_dtm;

    #[test]
    fn test_dtm_from_json() {
        test_2pc_dtm("test_2pc_dtm_from_json".to_string(), 9111, 3, Err("2pc_trace.json".to_string()));
    }
}