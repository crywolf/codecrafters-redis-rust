const DEFAULT_ADDRESS: &str = "127.0.0.1";
const DEFAULT_PORT: &str = "6379";

pub struct Config {
    pub addr: String,
    pub port: String,
    pub master_addr: Option<String>,
    pub master_port: Option<String>,
}

impl Config {
    pub fn new() -> Self {
        Self {
            addr: DEFAULT_ADDRESS.to_owned(),
            port: DEFAULT_PORT.to_owned(),
            master_addr: None,
            master_port: None,
        }
    }

    pub fn get_address(&self) -> String {
        format!("{}:{}", self.addr, self.port)
    }

    pub fn is_replica(&self) -> bool {
        self.master_addr.is_some() && self.master_port.is_some()
    }
}
