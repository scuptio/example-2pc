#[cfg(test)]
mod tests {
    use crate::test_2pc_dtm::tests::test_2pc_dtm;

    #[test]
    fn test() {
        for i in 1..=4 {
            let file = format!("2pc_trace_{}.db", i);
            test_2pc_dtm(9222, 3, Ok(file));
        }
    }
}