#[cfg(test)]
pub mod tests {
    use project_root::get_project_root;

    pub fn test_data_path(file:String) -> String {
        let proj_path = get_project_root().unwrap();
        let path = proj_path.join(format!("data/{}", file));
        path.to_str().unwrap().to_string()
    }
}