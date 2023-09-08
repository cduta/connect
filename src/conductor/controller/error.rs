use std::{sync::mpsc::{SendError, RecvError, TryRecvError}, fmt, error, io};

use super::{output, input, state};

pub enum IOError {
  Terminal(io::Error),
  ControlInputPayloadSend(SendError<input::InputControlPayload>),
  InputControlPayloadSend(SendError<input::ControlInputPayload>),
  ControlOutputPayloadSend(SendError<output::ControlOutputPayload>),
  OutputControlPayloadSend(SendError<output::OutputControlPayload>),
  ControlStatePayloadSend(SendError<state::ControlStatePayload>),
  StateControlPayloadSend(SendError<state::StateControlPayload>),
  PayloadRecv(RecvError),
  TryPayloadRecv(TryRecvError),
  DuckDB(duckdb::Error)
}

impl fmt::Debug for IOError {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match self {
      Self::Terminal(e)                 => write!(f, "Terminal {}",                 e),
      Self::ControlInputPayloadSend(e)  => write!(f, "ControlInputPayloadSend {}",  e),
      Self::InputControlPayloadSend(e)  => write!(f, "InputControlPayloadSend {}",  e),
      Self::ControlOutputPayloadSend(e) => write!(f, "ControlOutputPayloadSend {}", e),
      Self::OutputControlPayloadSend(e) => write!(f, "OutputControlPayloadSend {}", e),
      Self::ControlStatePayloadSend(e)  => write!(f, "ControlStatePayloadSend {}",  e),
      Self::StateControlPayloadSend(e)  => write!(f, "StateControlPayloadSend {}",  e),
      Self::PayloadRecv(e)              => write!(f, "PayloadRecv {}",              e),
      Self::TryPayloadRecv(e)           => write!(f, "TryPayloadRecv {}",           e),
      Self::DuckDB(e)                   => write!(f, "DuckDB {}",                   e)
    }
  }
}

impl fmt::Display for IOError {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match self {
      Self::Terminal(e)                 => write!(f, "Terminal {}",                 e),
      Self::ControlInputPayloadSend(e)  => write!(f, "ControlInputPayloadSend {}",  e),
      Self::InputControlPayloadSend(e)  => write!(f, "InputControlPayloadSend {}",  e),
      Self::ControlOutputPayloadSend(e) => write!(f, "ControlOutputPayloadSend {}", e),
      Self::OutputControlPayloadSend(e) => write!(f, "OutputControlPayloadSend {}", e),
      Self::ControlStatePayloadSend(e)  => write!(f, "ControlStatePayloadSend {}",  e),
      Self::StateControlPayloadSend(e)  => write!(f, "StateControlPayloadSend {}",  e),
      Self::PayloadRecv(e)              => write!(f, "PayloadRecv {}",              e),
      Self::TryPayloadRecv(e)           => write!(f, "TryPayloadRecv {}",           e),
      Self::DuckDB(e)                   => write!(f, "DuckDB {}",                   e)
    }
  }
}

impl error::Error for IOError {
  fn source(&self) -> Option<&(dyn error::Error + 'static)> {
    match self {
      Self::Terminal(ref e)                 => Some(e),
      Self::ControlInputPayloadSend(ref e)  => Some(e),
      Self::InputControlPayloadSend(ref e)  => Some(e),
      Self::ControlOutputPayloadSend(ref e) => Some(e),
      Self::OutputControlPayloadSend(ref e) => Some(e),
      Self::ControlStatePayloadSend(ref e)  => Some(e),
      Self::StateControlPayloadSend(ref e)  => Some(e),
      Self::PayloadRecv(ref e)              => Some(e),
      Self::TryPayloadRecv(ref e)           => Some(e),
      Self::DuckDB(ref e)                   => Some(e),
    }
  }
}

impl From<io::Error> for IOError {
  fn from(e: io::Error) -> Self { Self::Terminal(e) }
}

impl From<SendError<input::InputControlPayload>> for IOError {
  fn from(e: SendError<input::InputControlPayload>) -> Self { Self::ControlInputPayloadSend(e) }
}

impl From<SendError<input::ControlInputPayload>> for IOError {
  fn from(e: SendError<input::ControlInputPayload>) -> Self { Self::InputControlPayloadSend(e) }
}

impl From<SendError<output::ControlOutputPayload>> for IOError {
  fn from(e: SendError<output::ControlOutputPayload>) -> Self { Self::ControlOutputPayloadSend(e) }
}

impl From<SendError<output::OutputControlPayload>> for IOError {
  fn from(e: SendError<output::OutputControlPayload>) -> Self { Self::OutputControlPayloadSend(e) }
}

impl From<SendError<state::ControlStatePayload>> for IOError {
  fn from(e: SendError<state::ControlStatePayload>) -> Self { Self::ControlStatePayloadSend(e) }
}

impl From<SendError<state::StateControlPayload>> for IOError {
  fn from(e: SendError<state::StateControlPayload>) -> Self { Self::StateControlPayloadSend(e) }
}

impl From<RecvError> for IOError {
  fn from(e: RecvError) -> Self { Self::PayloadRecv(e) }
}

impl From<TryRecvError> for IOError {
  fn from(e: TryRecvError) -> Self { Self::TryPayloadRecv(e) }
}

impl From<duckdb::Error> for IOError {
  fn from(e: duckdb::Error) -> Self { Self::DuckDB(e) }
}

pub type IOResult = Result<(), IOError>;