use std::fs;
use std::path::Path;

const LOG_CONFIG_FILE_PATH: &str = "log-config.yml";
const CURRENT_LOG_FILE_PATH: &str = "log/current.log";

pub struct Logger { archive_path: String }

impl Logger {
  pub fn new(archive_path: String) -> Self { Logger { archive_path } }

  pub fn start(&self) -> Result<(), anyhow::Error> {
    log4rs::init_file(LOG_CONFIG_FILE_PATH, Default::default()).map_err(
      |err| {
        println!("Failed to read {}: {}", LOG_CONFIG_FILE_PATH, err);
        err
      }
    )?;

    Ok(())
  }

  pub fn stop(self) {
    if Path::new(CURRENT_LOG_FILE_PATH).exists() {
      if let Err(e) = fs::copy(CURRENT_LOG_FILE_PATH, self.archive_path) {
        println!("{:?}", e)
      }
    }
  }
}