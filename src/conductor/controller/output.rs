use crossterm::{cursor, event};
use crossterm::style::{Print, Color, SetForegroundColor, ResetColor};
use crossterm::terminal;
use std::fmt::Display;
use std::io::{Stdout, Write, self, stdout};
use std::str::FromStr;
use std::sync::mpsc::{Receiver, Sender, self, SyncSender};
use std::time;

use crate::common;

use super::error;

const SYNC_BUFFER_SIZE    : usize = 0;
const OUTPUT_RATE_IN_MSECS: u64   = 1;

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind { None, Wide, Door, Volatile, Removed }

impl ToString for Kind {
  fn to_string(&self) -> String {
    match self {
      Self::None     => "None".to_string(),
      Self::Wide     => "Wide".to_string(),
      Self::Door     => "Door".to_string(),
      Self::Volatile => "Volatile".to_string(),
      Self::Removed  => "Removed".to_string()
    }
  }
}

impl FromStr for Kind {
  type Err = String;
  fn from_str(s: &str) -> Result<Self, Self::Err> {
    match s {
      "None"     => Ok(Self::None),
      "Wide"     => Ok(Self::Wide),
      "Door"     => Ok(Self::Door),
      "Volatile" => Ok(Self::Volatile),
      "Removed"  => Ok(Self::Removed),
      _          => Err("Failed to parse string to type `Kind`".to_string())
    }
  }
}

#[allow(clippy::upper_case_acronyms,dead_code)]
#[derive(PartialEq, Eq)]
pub enum Literal { Unknown,Empty,Wall,Volatile,Object(i32,Kind),String(String) }

impl Display for Literal {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Literal::Empty                         => write!(f, " "),
      Literal::Wall                          => write!(f, "█"),
      Literal::Volatile                      => write!(f, "◊"),

      Literal::Object(0b00001000,Kind::None) => write!(f, "╵"),
      Literal::Object(0b10000000,Kind::Wide) => write!(f, "╹"),

      Literal::Object(0b00000100,Kind::None) => write!(f, "╶"),
      Literal::Object(0b01000000,Kind::Wide) => write!(f, "╺"),

      Literal::Object(0b00000010,Kind::None) => write!(f, "╷"),
      Literal::Object(0b00100000,Kind::Wide) => write!(f, "╻"),

      Literal::Object(0b00000001,Kind::None) => write!(f, "╴"),
      Literal::Object(0b00010000,Kind::Wide) => write!(f, "╸"),

      Literal::Object(0b00001100,Kind::None) => write!(f, "└"),
      Literal::Object(0b10000100,Kind::Wide) => write!(f, "┖"),
      Literal::Object(0b01001000,Kind::Wide) => write!(f, "┕"),
      Literal::Object(0b11000000,Kind::Wide) => write!(f, "┗"),
      Literal::Object(0b10000100,Kind::Door) => write!(f, "╙"),
      Literal::Object(0b01001000,Kind::Door) => write!(f, "╘"),
      Literal::Object(0b11000000,Kind::Door) => write!(f, "╚"),

      Literal::Object(0b00000110,Kind::None) => write!(f, "┌"),
      Literal::Object(0b01000010,Kind::Wide) => write!(f, "┍"),
      Literal::Object(0b00100100,Kind::Wide) => write!(f, "┎"),
      Literal::Object(0b01100000,Kind::Wide) => write!(f, "┏"),
      Literal::Object(0b01000010,Kind::Door) => write!(f, "╒"),
      Literal::Object(0b00100100,Kind::Door) => write!(f, "╓"),
      Literal::Object(0b01100000,Kind::Door) => write!(f, "╔"),

      Literal::Object(0b00000011,Kind::None) => write!(f, "┐"),
      Literal::Object(0b00100001,Kind::Wide) => write!(f, "┒"),
      Literal::Object(0b00010010,Kind::Wide) => write!(f, "┑"),
      Literal::Object(0b00110000,Kind::Wide) => write!(f, "┓"),
      Literal::Object(0b00100001,Kind::Door) => write!(f, "╖"),
      Literal::Object(0b00010010,Kind::Door) => write!(f, "╕"),
      Literal::Object(0b00110000,Kind::Door) => write!(f, "╗"),

      Literal::Object(0b00001001,Kind::None) => write!(f, "┘"),
      Literal::Object(0b10000001,Kind::Wide) => write!(f, "┚"),
      Literal::Object(0b00011000,Kind::Wide) => write!(f, "┙"),
      Literal::Object(0b10010000,Kind::Wide) => write!(f, "┛"),
      Literal::Object(0b10000001,Kind::Door) => write!(f, "╜"),
      Literal::Object(0b00011000,Kind::Door) => write!(f, "╛"),
      Literal::Object(0b10010000,Kind::Door) => write!(f, "╝"),

      Literal::Object(0b00001010,Kind::None) => write!(f, "│"),
      Literal::Object(0b10000010,Kind::Wide) => write!(f, "╿"),
      Literal::Object(0b00101000,Kind::Wide) => write!(f, "╽"),
      Literal::Object(0b10100000,Kind::Wide) => write!(f, "┃"),
      Literal::Object(0b10100000,Kind::Door) => write!(f, "║"),

      Literal::Object(0b00000101,Kind::None) => write!(f, "─"),
      Literal::Object(0b01000001,Kind::Wide) => write!(f, "╼"),
      Literal::Object(0b00010100,Kind::Wide) => write!(f, "╾"),
      Literal::Object(0b01010000,Kind::Wide) => write!(f, "━"),
      Literal::Object(0b01010000,Kind::Door) => write!(f, "═"),

      Literal::Object(0b00001110,Kind::None) => write!(f, "├"),
      Literal::Object(0b10000110,Kind::Wide) => write!(f, "┞"),
      Literal::Object(0b01001010,Kind::Wide) => write!(f, "┝"),
      Literal::Object(0b00101100,Kind::Wide) => write!(f, "┟"),
      Literal::Object(0b11000010,Kind::Wide) => write!(f, "┡"),
      Literal::Object(0b01101000,Kind::Wide) => write!(f, "┢"),
      Literal::Object(0b10100100,Kind::Wide) => write!(f, "┠"),
      Literal::Object(0b11100000,Kind::Wide) => write!(f, "┣"),
      Literal::Object(0b01001010,Kind::Door) => write!(f, "╞"),
      Literal::Object(0b10100100,Kind::Door) => write!(f, "╟"),
      Literal::Object(0b11100000,Kind::Door) => write!(f, "╠"),

      Literal::Object(0b00000111,Kind::None) => write!(f, "┬"),
      Literal::Object(0b01000011,Kind::Wide) => write!(f, "┮"),
      Literal::Object(0b00100101,Kind::Wide) => write!(f, "┰"),
      Literal::Object(0b00010110,Kind::Wide) => write!(f, "┭"),
      Literal::Object(0b01100001,Kind::Wide) => write!(f, "┲"),
      Literal::Object(0b00110100,Kind::Wide) => write!(f, "┱"),
      Literal::Object(0b01010010,Kind::Wide) => write!(f, "┯"),
      Literal::Object(0b01110000,Kind::Wide) => write!(f, "┳"),
      Literal::Object(0b00100101,Kind::Door) => write!(f, "╥"),
      Literal::Object(0b01010010,Kind::Door) => write!(f, "╤"),
      Literal::Object(0b01110000,Kind::Door) => write!(f, "╦"),

      Literal::Object(0b00001011,Kind::None) => write!(f, "┤"),
      Literal::Object(0b10000011,Kind::Wide) => write!(f, "┦"),
      Literal::Object(0b00101001,Kind::Wide) => write!(f, "┧"),
      Literal::Object(0b00011010,Kind::Wide) => write!(f, "┥"),
      Literal::Object(0b10010010,Kind::Wide) => write!(f, "┩"),
      Literal::Object(0b10100001,Kind::Wide) => write!(f, "┨"),
      Literal::Object(0b00111000,Kind::Wide) => write!(f, "┪"),
      Literal::Object(0b10110000,Kind::Wide) => write!(f, "┫"),
      Literal::Object(0b10100001,Kind::Door) => write!(f, "╢"),
      Literal::Object(0b00011010,Kind::Door) => write!(f, "╡"),
      Literal::Object(0b10110000,Kind::Door) => write!(f, "╣"),

      Literal::Object(0b00001101,Kind::None) => write!(f, "┴"),
      Literal::Object(0b10000101,Kind::Wide) => write!(f, "┸"),
      Literal::Object(0b01001001,Kind::Wide) => write!(f, "┶"),
      Literal::Object(0b00011100,Kind::Wide) => write!(f, "┵"),
      Literal::Object(0b11000001,Kind::Wide) => write!(f, "┺"),
      Literal::Object(0b01011000,Kind::Wide) => write!(f, "┷"),
      Literal::Object(0b10010100,Kind::Wide) => write!(f, "┹"),
      Literal::Object(0b11010000,Kind::Wide) => write!(f, "┻"),
      Literal::Object(0b10000101,Kind::Door) => write!(f, "╨"),
      Literal::Object(0b01011000,Kind::Door) => write!(f, "╧"),
      Literal::Object(0b11010000,Kind::Door) => write!(f, "╩"),

      Literal::Object(0b00001111,Kind::None) => write!(f, "┼"),
      Literal::Object(0b10000111,Kind::Wide) => write!(f, "╀"),
      Literal::Object(0b01001011,Kind::Wide) => write!(f, "┾"),
      Literal::Object(0b00101101,Kind::Wide) => write!(f, "╁"),
      Literal::Object(0b00011110,Kind::Wide) => write!(f, "┽"),
      Literal::Object(0b11000011,Kind::Wide) => write!(f, "╄"),
      Literal::Object(0b10100101,Kind::Wide) => write!(f, "╂"),
      Literal::Object(0b10010110,Kind::Wide) => write!(f, "╃"),
      Literal::Object(0b01101001,Kind::Wide) => write!(f, "╆"),
      Literal::Object(0b01011010,Kind::Wide) => write!(f, "┿"),
      Literal::Object(0b00111100,Kind::Wide) => write!(f, "╅"),
      Literal::Object(0b11100001,Kind::Wide) => write!(f, "╊"),
      Literal::Object(0b11010010,Kind::Wide) => write!(f, "╇"),
      Literal::Object(0b10110100,Kind::Wide) => write!(f, "╉"),
      Literal::Object(0b01111000,Kind::Wide) => write!(f, "╈"),
      Literal::Object(0b11110000,Kind::Wide) => write!(f, "╋"),
      Literal::Object(0b10100101,Kind::Door) => write!(f, "╫"),
      Literal::Object(0b01011010,Kind::Door) => write!(f, "╪"),
      Literal::Object(0b11110000,Kind::Door) => write!(f, "╬"),
      Literal::String(s)                => write!(f, "{}", s),
      _                                 => write!(f, "?")
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