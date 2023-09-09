use crossterm::{cursor, event};
use crossterm::style::{Print, Color, SetForegroundColor, ResetColor};
use crossterm::terminal;
use std::fmt::Display;
use std::io::{Stdout, Write, self, stdout};
use std::sync::mpsc::{Receiver, Sender, self, SyncSender};
use std::time;

use crate::common;

use super::error;

const SYNC_BUFFER_SIZE    : usize = 0;
const OUTPUT_RATE_IN_MSECS: u64 = 1;

#[allow(clippy::upper_case_acronyms)]
#[derive(PartialEq, Eq)]
pub enum Literal { Unknown,Empty,Wall,U,R,D,L,UR,RD,DL,UL,UD,RL,URD,RDL,UDL,URL,URDL,String(String) }

impl Display for Literal {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {  
    match self {
      Literal::Unknown   => write!(f, "?"),
      Literal::Empty     => write!(f, " "),
      Literal::Wall      => write!(f, "█"),
      Literal::U         => write!(f, "╵"),
      Literal::R         => write!(f, "╶"),
      Literal::D         => write!(f, "╷"),
      Literal::L         => write!(f, "╴"),
      Literal::UR        => write!(f, "└"),
      Literal::RD        => write!(f, "┌"),
      Literal::DL        => write!(f, "┐"),
      Literal::UL        => write!(f, "┘"),
      Literal::UD        => write!(f, "│"),
      Literal::RL        => write!(f, "─"),
      Literal::URD       => write!(f, "├"),
      Literal::RDL       => write!(f, "┬"),
      Literal::UDL       => write!(f, "┤"),
      Literal::URL       => write!(f, "┴"),
      Literal::URDL      => write!(f, "┼"),
      Literal::String(s) => write!(f, "{}", s)
    }
  }
}

#[derive(PartialEq, Eq)]
pub struct Char {
  l    : Literal,
  pos  : (u16,u16),
  color: Option<Color>
}

impl Char {
  pub fn new(l: Literal, pos: (u16,u16), color: Option<Color>) -> Self { Char { l, pos, color } }
}

#[derive(PartialEq, Eq)]
pub enum ControlOutputPayload { ClearTerminal, PrintChars(Vec<Char>), SetCursorPosition((u16,u16)), ResizeTerminal((u16,u16)), Shutdown }

#[derive(PartialEq, Eq)]
pub enum OutputControlPayload { ReportTerminalSize((u16,u16)) }

pub struct Output {
  output_control_send: Sender<OutputControlPayload>,
  control_output_recv: Receiver<ControlOutputPayload>,
  stdout             : Stdout
}

impl Output {
  pub fn new() -> io::Result<(Self, SyncSender<ControlOutputPayload>, Receiver<OutputControlPayload>)> {
    let (control_output_send, control_output_recv) = mpsc::sync_channel(SYNC_BUFFER_SIZE);
    let (output_control_send, output_control_recv) = mpsc::channel();
    Ok((Self { output_control_send, control_output_recv, stdout: stdout() }, control_output_send, output_control_recv))
  }
  fn  init(&mut self) -> error::IOResult {
    // Raw mode i.e. terminal does not process keyboard inputs before we receive them
    terminal::enable_raw_mode()?;

    // Execute terminal commands:
    execute!(self.stdout, terminal::Clear(terminal::ClearType::All))?;

    // Move cursor to top-left corner and set style
    execute!(self.stdout, cursor::MoveTo(0, 0), cursor::SetCursorStyle::SteadyBlock)?;

    // Enable mouse events
    execute!(self.stdout, event::EnableMouseCapture)?;

    // Report the terminal size
    self.output_control_send.send(OutputControlPayload::ReportTerminalSize(terminal::size()?))?;

    Ok(())
  }
  fn clear_terminal(&mut self) -> error::IOResult {
    execute!(self.stdout, terminal::Clear(terminal::ClearType::All), terminal::Clear(terminal::ClearType::Purge))?;
    Ok(())
  }
  fn print_chars(&mut self, chars: Vec<Char>) -> error::IOResult {
      queue!(self.stdout, cursor::SavePosition)?;
      for Char { l, pos: (x,y), color } in chars { 
        if let Some(c) = color {
          queue!(self.stdout, SetForegroundColor(c))?;
        }
        queue!(self.stdout, cursor::MoveTo(x,y), Print(l), ResetColor)?;
      }
      queue!(self.stdout, cursor::RestorePosition)?;
      self.stdout.flush()?;
      Ok(())
  }
  pub fn print(&mut self) -> error::IOResult {
    let mut now = time::Instant::now();
    self.init()?;
    loop {
      // Make sure, we wait to keep output rate consistent
      common::wait_minus_elapsed(time::Duration::from_millis(OUTPUT_RATE_IN_MSECS), now.elapsed());
      now = time::Instant::now();

      match self.control_output_recv.recv()? {
        ControlOutputPayload::ClearTerminal            => self.clear_terminal()?,
        ControlOutputPayload::PrintChars(chars)        => self.print_chars(chars)?,
        ControlOutputPayload::SetCursorPosition((x,y)) => execute!(self.stdout, cursor::MoveTo(x,y))?,
        ControlOutputPayload::ResizeTerminal((w,h))    => execute!(self.stdout, terminal::SetSize(w,h))?,
        ControlOutputPayload::Shutdown                 => break
      }
    }
    self.shutdown()
  }
  fn shutdown(&mut self) -> error::IOResult {
    // Reset cursor
    execute!(self.stdout, cursor::MoveTo(0,0), cursor::SetCursorStyle::SteadyBlock, event::DisableMouseCapture)?;
    terminal::disable_raw_mode()?;
    Ok(())
  }
}