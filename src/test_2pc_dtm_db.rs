#[cfg(test)]
mod tests {
    use crate::test_2pc_dtm::tests::test_2pc_dtm;

    #[test]
    fn test_dtm_from_trace_db() {
        for i in 1..=4 {
            let file = format!("2pc_trace_{}.db", i);
            test_2pc_dtm("test_2pc_dtm_from_trace_db".to_string(), 9222, 3, Ok(file));
        }
    }
}