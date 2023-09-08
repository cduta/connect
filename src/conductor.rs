mod logger;
mod controller;

use log::{trace, error};
use chrono::offset::Local;
use chrono::DateTime;
use std::result::Result;
use std::thread;
use std::time::SystemTime;

pub fn run() -> Result<(), anyhow::Error> {

  let start_time = Into::<DateTime<Local>>::into(SystemTime::now()).format("%Y-%m-%d_%H-%M-%S").to_string();
  let logger = logger::Logger::new(format!("log/{}.log", start_time));

  logger.start()?;

  let controller_thread = thread::spawn(controller::run);

  trace!("Init...OK");

  if let Err(e) = controller_thread.join() {
    error!("Controller terminated with an error: {:?}", e.downcast_ref::<&str>());
  }

  trace!("Shutdown...OK");

  logger.stop();

  Ok(())
}