mod error;
mod input;
mod output;
mod state;

use std::path::Path;
use std::str::FromStr;
use std::sync::mpsc::SendError;
use std::time;
use std::{thread::{self, JoinHandle}, sync::mpsc::{SyncSender, Receiver, TryRecvError}};

use crossterm::event::{KeyEvent, KeyCode, KeyModifiers, KeyEventKind, KeyEventState};
use clap::Parser;
use crossterm::style::Color;
use input::Input;
use output::{Literal, Char, Output};
use state::State;
use log::error;

use crate::common;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
  // Path to `.lvl` file
  #[arg(short = 'l', long)] level: String,
  // undo size
  #[arg(short = 'u', long, default_value = "250")] undo: usize
}

#[derive(PartialEq, Eq)]
enum ExecutionState { Run, Restart, Error, Quit }

const FORWARDING_RATE_IN_MSECS: u64   = 1;
const QUICK_SHUTDOWN_IN_SECS  : u64   = 5;
const TOO_MANY_QUICK_SHUTDOWNS: usize = 5;

struct Controller {
  control_input_send : SyncSender<input::ControlInputPayload>,
  control_output_send: SyncSender<output::ControlOutputPayload>,
  control_state_send : SyncSender<state::ControlStatePayload>,
  input_control_recv : Receiver<input::InputControlPayload>,
  output_control_recv: Receiver<output::OutputControlPayload>,
  state_control_recv : Receiver<state::StateControlPayload>,
  input_thread       : JoinHandle<error::IOResult>,
  state_thread       : JoinHandle<error::IOResult>,
  output_thread      : JoinHandle<error::IOResult>
}

impl Controller {
  fn new(args: &Args) -> Result<Self,error::IOError> {
    let (    input , control_input_send , input_control_recv ) = Input::new();
    let (    state , control_state_send , state_control_recv ) = State::new_with_args(args.level.clone(), args.undo)?;
    let (mut output, control_output_send, output_control_recv) = Output::new()?;
    Ok(Self {
      control_input_send, control_output_send, control_state_send,
      input_control_recv, output_control_recv, state_control_recv,
      input_thread : thread::spawn(move || input.capture().map_err( |err| { error!("Input thread shutdown with error: {}",  err); err })),
      state_thread : thread::spawn(move || state.maintain().map_err(|err| { error!("State thread shutdown with error: {}",  err); err })),
      output_thread: thread::spawn(move || output.print().map_err(  |err| { error!("Output thread shutdown with error: {}", err); err }))
    })
  }

  /// Map key event onto its respective payload and send it to output
  /// Quit on pressing `q`
  #[inline]
  fn update_state_on_key_event(&self, key_event: KeyEvent) -> Result<ExecutionState, error::IOError> {
    match if let KeyEvent { code, modifiers: KeyModifiers::NONE, kind: KeyEventKind::Press, state: KeyEventState::NONE } = key_event { Some(code) } else { None } {
      Some(KeyCode::Char('q')) => { return Ok(ExecutionState::Quit);                                                                   },
      Some(KeyCode::Char('8'))    |
      Some(KeyCode::Up)        => { self.control_state_send.send(state::ControlStatePayload::MoveCursor(state::Direction::Up))?;       },
      Some(KeyCode::Char('9')) => { self.control_state_send.send(state::ControlStatePayload::MoveCursor(state::Direction::UpRight))?;  },
      Some(KeyCode::Char('6'))    |
      Some(KeyCode::Right    ) => { self.control_state_send.send(state::ControlStatePayload::MoveCursor(state::Direction::Right))?;    },
      Some(KeyCode::Char('3')) => { self.control_state_send.send(state::ControlStatePayload::MoveCursor(state::Direction::DownRight))?;},
      Some(KeyCode::Char('2'))    |
      Some(KeyCode::Down     ) => { self.control_state_send.send(state::ControlStatePayload::MoveCursor(state::Direction::Down))?;     },
      Some(KeyCode::Char('1')) => { self.control_state_send.send(state::ControlStatePayload::MoveCursor(state::Direction::DownLeft))?; },
      Some(KeyCode::Char('4'))    |
      Some(KeyCode::Left     ) => { self.control_state_send.send(state::ControlStatePayload::MoveCursor(state::Direction::Left))?;     },
      Some(KeyCode::Char('7')) => { self.control_state_send.send(state::ControlStatePayload::MoveCursor(state::Direction::UpLeft))?;   },
      Some(KeyCode::Char('5'))    |
      Some(KeyCode::Char(' '))    |
      Some(KeyCode::Enter    ) => { self.control_state_send.send(state::ControlStatePayload::Select)?;                                 },
      Some(KeyCode::Char('n')) => { return Ok(ExecutionState::Restart);                                                                },
      Some(KeyCode::Char('u')) => { self.control_state_send.send(state::ControlStatePayload::Undo)?;                                   },
      Some(KeyCode::Char('r')) => { self.control_state_send.send(state::ControlStatePayload::Redo)?;                                   },
      Some(KeyCode::Char('s')) => { self.control_state_send.send(state::ControlStatePayload::Save)?;                                   },
      Some(KeyCode::Char('l')) => { self.control_state_send.send(state::ControlStatePayload::Load)?;                                   },
      _ => ()
    }
    Ok(ExecutionState::Run)
  }

  ///
  #[inline]
  fn connectors_to_literal(connectors: i32, kind: output::Kind) -> output::Literal {
    match (connectors, kind) {
      (_         ,output::Kind::Removed ) => { output::Literal::Empty },
      (_         ,output::Kind::Volatile) => { output::Literal::Volatile },
      (0         ,_                     ) => { output::Literal::Wall  },
      (connectors,kind                  ) => { output::Literal::Object(connectors,kind) }
    }
  }

  /// Determine the next exeuction state based on payloads sent by child processes
  fn next_exec_state(&mut self) -> ExecutionState {
    #[inline]
    fn send_handler<T>(mut exec_state: ExecutionState, result: Result<(), SendError<T>>, error_msg: &str) -> ExecutionState {
      match result { Ok(()) => (), Err(e) => { error!("{}: {}", error_msg, e); exec_state = ExecutionState::Error; } }; exec_state
    }

    let mut exec_state = ExecutionState::Run;

    exec_state = match self.input_control_recv.try_recv() {
      // Interpret input and forward it to all listening threads
      Ok(input::InputControlPayload::Key(key_event)) => {
        self.update_state_on_key_event(key_event)
          .unwrap_or_else(|e| { error!("Error updating state on key event: {}", e); ExecutionState::Error })
      },
      Ok(input::InputControlPayload::Mouse(pos)) => {
        send_handler(exec_state, self.control_state_send.send(state::ControlStatePayload::SetCursorPosition(pos)), "Error sending set cursor position event to state")
      },
      Ok(input::InputControlPayload::Resize(size)) => {
        send_handler(exec_state, self.control_state_send.send(state::ControlStatePayload::SetBoardSize(size)), "Error sending resize event to state")
      },
      // If the input disconnects, we take it as an error
      Err(TryRecvError::Disconnected) => { ExecutionState::Error },
      _                               => { exec_state }
    };

    exec_state = match self.state_control_recv.try_recv() {
      Ok(state::StateControlPayload::ClearTerminal) => {
        send_handler(exec_state, self.control_output_send.send(output::ControlOutputPayload::ClearTerminal), "Error clearing terminal")
      },
      Ok(state::StateControlPayload::PrintObjects(objects)) => {
        send_handler(exec_state, self.control_output_send.send(output::ControlOutputPayload::PrintChars(
          objects.into_iter()
            .map(|obj| Char::new(
              Controller::connectors_to_literal(obj.connectors(),output::Kind::from_str(obj.kind().as_str()).unwrap_or(output::Kind::None)),
              obj.pos(),
              obj.color()))
            .collect()
        )), "Error printing objects to output")
      },
      Ok(state::StateControlPayload::SetCursorPosition((x,y))) => {
        send_handler(exec_state, self.control_output_send.send(output::ControlOutputPayload::SetCursorPosition((x,y))), "Error sending cursor position to output")
      },
      Ok(state::StateControlPayload::MoveShape(here_shape,there_shape)) => {
        send_handler(exec_state, self.control_output_send.send(output::ControlOutputPayload::PrintChars({
          here_shape.into_iter()
            .map(|obj| Char::new(Literal::Empty, obj.pos(), None))
            .chain(there_shape.into_iter()
                     .map(|obj| Char::new(
                      Controller::connectors_to_literal(obj.connectors(), output::Kind::from_str(obj.kind().as_str()).unwrap_or(output::Kind::None)),
                      obj.pos(),
                      obj.color())))
            .collect()
        })), "Error sending shapes that move from here to there to output")
      },
      Ok(state::StateControlPayload::ResizeTerminal(size)) => {
        send_handler(exec_state, self.control_output_send.send(output::ControlOutputPayload::ResizeTerminal(size)), "Error resizing terminal")
      },
      Ok(state::StateControlPayload::TurnCounter(y_pos, turn, complete)) => {
        send_handler(exec_state, self.control_output_send.send(
          output::ControlOutputPayload::PrintChars(vec![
            Char::new(Literal::String(format!("Turn: {}{}", turn, if complete {if cfg!(windows) {" OK"} else {" ✓"}} else {"   "})), (1,y_pos), if complete {Some(Color::Green)} else {Some(Color::DarkGrey)})]
          )
        ), "Error printing `level complete`")
      },
      Err(TryRecvError::Disconnected) => { ExecutionState::Error },
      _                               => { exec_state }
    };

    exec_state = match self.output_control_recv.try_recv() {
      Ok(output::OutputControlPayload::ReportTerminalSize(terminal_size)) => {
        send_handler(exec_state, self.control_state_send.send(state::ControlStatePayload::SetBoardSize(terminal_size)), format!("Error reporting the terminal size ({:?}) from output to state", terminal_size).as_str())
      },
      Err(TryRecvError::Disconnected) => { ExecutionState::Error },
      _                               => { exec_state }
    };

    if self.input_thread.is_finished()
    || self.state_thread.is_finished()
    || self.output_thread.is_finished() { exec_state = ExecutionState::Error }
    exec_state
  }

  fn start(&mut self) -> ExecutionState {
    let mut exec_state = ExecutionState::Run;
    let mut now = time::Instant::now();
    while exec_state == ExecutionState::Run {
      // Make sure, we wait to keep forwarding rate consistent
      common::wait_minus_elapsed(time::Duration::from_millis(FORWARDING_RATE_IN_MSECS), now.elapsed());
      now = time::Instant::now();
      // Determine next execution state
      exec_state = self.next_exec_state();
    }
    exec_state
  }

  fn shutdown(self) {
    #[inline]
    fn shutdown_thread<T>(thread: JoinHandle<error::IOResult>, sender: Result<(), SendError<T>>) {
      match sender {
        Ok(_)  => if let Err(e) = thread.join() { error!("Error on input thread join: {:?}", e.downcast_ref::<&str>())},
        Err(e) => {
          if thread.is_finished()  { if let Err(e) = thread.join() { error!("Error in input thread {:?}", e.downcast_ref::<&str>()) } }
          error!("Error on input shutdown: {}", e);
        }
      }
    }
    // Clean shutdown of input thread, if possible
    shutdown_thread(self.input_thread, self.control_input_send.send(input::ControlInputPayload::Shutdown));
    // Clean shutdown of state thread, if possible
    shutdown_thread(self.state_thread, self.control_state_send.send(state::ControlStatePayload::Shutdown));
    // Clean shutdown of output thread, if possible
    shutdown_thread(self.output_thread, self.control_output_send.send(output::ControlOutputPayload::Shutdown));
  }
}

pub fn run() {
  let args = Args::parse();
  if !Path::new(args.level.as_str()).exists() {
    let error_message = format!("Level `{}` does not exist", args.level);
    error!("{error_message}");
    println!("{error_message}");
    return;
  }
  let mut controller = match Controller::new(&args) {
    Ok(controller) => controller,
    Err(e) => {
      let error_message = format!("Could not create the controller: {}", e);
      error!("{error_message}");
      println!("{error_message}");
      return;
    }
  };
  let mut quick_shutdowns = 0;
  let mut now = time::Instant::now();
  // Make sure to stop execution, if too many shutdowns happen in quick succession
  loop {
    match controller.start() {
      ExecutionState::Quit    => { controller.shutdown(); break },
      ExecutionState::Restart => {
        controller.shutdown();
        controller = match Controller::new(&args) {
          Ok(controller) => controller,
          Err(e)         => {
            error!("Could not create the controller: {}", e);
            return;
          }
        };
      },
      _                       => {
        // If less then QUICK_SHUTDOWN_IN_SECS time elapsed since the last shut down
        if now.elapsed() < time::Duration::from_secs(QUICK_SHUTDOWN_IN_SECS) { quick_shutdowns += 1; } else { quick_shutdowns = 0; }
        controller.shutdown();
        if quick_shutdowns >= TOO_MANY_QUICK_SHUTDOWNS {
          error!("Execution stopped: Too many shutdowns ({}) in succession", quick_shutdowns);
          break
        }
        controller = match Controller::new(&args) {
          Ok(controller) => controller,
          Err(e)         => {
            error!("Could not create the controller: {}", e);
            return;
          }
        };
        now = time::Instant::now();
      }
    }
  }
}