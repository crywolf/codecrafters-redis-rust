const DEFAULT_ADDRESS: &str = "127.0.0.1";
const DEFAULT_PORT: &str = "6379";

#[derive(Clone)]
pub struct Config {
    pub addr: String,
    pub port: String,
    pub master_addr: Option<String>,
    pub master_port: Option<String>,
    pub dir: Option<String>,
    pub db_filename: Option<String>,
}

impl Config {
    pub fn new() -> Self {
        Self {
            addr: DEFAULT_ADDRESS.to_owned(),
            port: DEFAULT_PORT.to_owned(),
            master_addr: None,
            master_port: None,
            dir: None,
            db_filename: None,
        }
    }

    pub fn get_address(&self) -> String {
        format!("{}:{}", self.addr, self.port)
    }

    pub fn get_master_address(&self) -> Option<String> {
        if self.is_replica() {
            return Some(format!(
                "{}:{}",
                self.master_addr.as_ref().expect("addr of master is set"),
                self.master_port.as_ref().expect("port of master is set")
            ));
        }
        None
    }

    pub fn is_replica(&self) -> bool {
        self.master_addr.is_some() && self.master_port.is_some()
    }
}
