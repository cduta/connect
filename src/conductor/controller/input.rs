use crossterm::event::{poll, read, Event, KeyEvent, MouseEvent, MouseEventKind};
use std::time;
use std::sync::mpsc::{SyncSender, Receiver, TryRecvError, self};

use crate::common;

use super::error::{self, IOError};

const SYNC_BUFFER_SIZE  : usize = 0;
const SENDING_RATE_IN_MS: u64   = 1;
const POLL_WAIT_IN_MS   : u64   = 1;

#[derive(PartialEq, Eq)]
pub enum InputControlPayload { Key(KeyEvent), Resize((u16,u16)), Mouse((u16,u16)) }

#[derive(PartialEq, Eq)]
pub enum ControlInputPayload { Shutdown }

pub struct Input {
  input_control_send: SyncSender<InputControlPayload>,
  control_input_recv: Receiver<ControlInputPayload>
}

impl Input {
  pub fn new() -> (Self, SyncSender<ControlInputPayload>, Receiver<InputControlPayload>) {
    let (control_input_send, control_input_recv)   = mpsc::sync_channel(SYNC_BUFFER_SIZE);
    let (input_control_send, input_control_recv)   = mpsc::sync_channel(SYNC_BUFFER_SIZE);
    (Self { input_control_send, control_input_recv }, control_input_send, input_control_recv)
  }
  fn read(&self) -> error::IOResult {
    if poll(time::Duration::from_millis(POLL_WAIT_IN_MS))? {
      match read()? {
        Event::Key(key_event)                                                                       => { self.input_control_send.send(InputControlPayload::Key(key_event))? },
        Event::Mouse(MouseEvent { kind: MouseEventKind::Down(_), column: x, row: y, modifiers: _ }) => { self.input_control_send.send(InputControlPayload::Mouse((x,y)))?   },
        Event::Mouse(MouseEvent { kind: MouseEventKind::Drag(_), column: x, row: y, modifiers: _ }) => { self.input_control_send.send(InputControlPayload::Mouse((x,y)))?   },
        Event::Resize(w,h)                                                                          => { self.input_control_send.send(InputControlPayload::Resize((w,h)))?  },
        Event::FocusGained                                                                          => (),
        Event::FocusLost                                                                            => (),
        Event::Mouse(_)                                                                             => (),
        Event::Paste(_)                                                                             => ()
      }
    }
    Ok(())
  }
  fn try_control_input_recv(&self) -> Result<Option<ControlInputPayload>, IOError> {
    match self.control_input_recv.try_recv() {
      Ok(payload)              => Ok(Some(payload)),
      Err(TryRecvError::Empty) => Ok(None),
      Err(e)                   => Err(IOError::TryPayloadRecv(e))
    }
  }
  pub fn capture(&self) -> error::IOResult {
    let mut now = time::Instant::now();
    loop {
      // Make sure, we wait to keep input rate consistent
      common::wait_minus_elapsed(time::Duration::from_millis(SENDING_RATE_IN_MS), now.elapsed());
      now = time::Instant::now();
      self.read()?;
      if let Some(ControlInputPayload::Shutdown) = self.try_control_input_recv()? { break }
    }
    Ok(())
  }
}