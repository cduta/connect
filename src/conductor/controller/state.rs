use std::{sync::mpsc::{Sender, SyncSender, Receiver, self}, time, cmp::max, fs, path::Path};
use crossterm::style::Color;
use duckdb::{Connection, params, OptionalExt, Statement};
use zip_archive::Archiver;

use crate::common;

use super::error;

type MoveObjectResult = Result<Option<(Vec<Object>,Vec<Object>)>, error::IOError>;

const SYNC_BUFFER_SIZE     : usize = 0;
const SENDING_RATE_IN_MSECS: u64   = 1;
const INITIAL_CURSOR_POS_X : u16   = 0;
const INITIAL_CURSOR_POS_Y : u16   = 0;
const INITIAL_BOARD_SIZE_W : u16   = u16::MAX;
const INITIAL_BOARD_SIZE_H : u16   = u16::MAX;
const UNDO_SIZE_IN_TURNS   : usize = 100;
const TEMP_SAVE_PATH       : &str  = "temp-save";
const SAVE_FILE_PATH       : &str  = "connect";

#[derive(PartialEq, Eq)]
pub enum Direction { Up, UpRight, Right, DownRight, Down, DownLeft, Left, UpLeft }

#[derive(PartialEq, Eq)]
pub enum ControlStatePayload { MoveCursor(Direction), SetCursorPosition((u16,u16)), Select, SetBoardSize((u16,u16)), Undo, Redo, Save, Load, Shutdown }

#[derive(PartialEq, Eq)]
pub enum StateControlPayload { ClearTerminal, PrintObjects(Vec<Object>), SetCursorPosition((u16,u16)), MoveShape(Vec<Object>,Vec<Object>), ResizeTerminal((u16,u16)) }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Object { id: i32, shape: i32, color: Option<Color>, connectors: i32, pos: (u16,u16) }

impl Object {
  fn new(id: i32, shape: i32, connectors: i32, pos: (u16,u16)) -> Self { Object { id, shape, connectors, pos, color: None } }
  fn new_with_color(id: i32, shape: i32, connectors: i32, pos: (u16,u16), color: Option<Color>) -> Self { Object { id, shape, connectors, pos, color } }
  pub fn connectors(&self) -> i32 { self.connectors }
  pub fn pos(&self) -> (u16,u16) { self.pos }
  pub fn color(&self) -> Option<Color> { self.color }
}

pub struct State {
  state_control_send: Sender<StateControlPayload>,
  control_state_recv: Receiver<ControlStatePayload>,
  level_path        : Option<String>,
  board_size        : (u16,u16),
  cursor_pos        : (u16,u16),
  selected_shape    : Option<i32>,
  db                : Connection
}

impl State {
  pub fn new() -> duckdb::Result<(Self, SyncSender<ControlStatePayload>, Receiver<StateControlPayload>)> {
    let (control_state_send, control_state_recv) = mpsc::sync_channel(SYNC_BUFFER_SIZE);
    let (state_control_send, state_control_recv) = mpsc::channel();
    Ok((Self { 
      state_control_send, 
      control_state_recv, 
      level_path       : None,
      board_size       : (INITIAL_BOARD_SIZE_W, INITIAL_BOARD_SIZE_H),
      cursor_pos       : (INITIAL_CURSOR_POS_X, INITIAL_CURSOR_POS_Y), 
      selected_shape   : None,
      db               : Connection::open_in_memory()?
    }, control_state_send, state_control_recv))
  }
  pub fn new_with_level_path(level_path: String) -> duckdb::Result<(Self, SyncSender<ControlStatePayload>, Receiver<StateControlPayload>)> {
    let (mut state, control_state_recv, state_control_send) = State::new()?;
    state.level_path = Some(level_path);
    Ok((state, control_state_recv, state_control_send))
  }
  #[inline]
  pub fn cursor_position(&self) -> (u16,u16) { self.cursor_pos }
  /// The database is initialized with a sequence `object_seq_id` and table `objects`
  fn init_database(&self) -> duckdb::Result<()> {
    self.db.execute_batch(r#"
    -- Is `x` ∈ { 0,…,65534 }?
    create macro is_inbound(x) as x between 0 and 65534;

    -- Is `x` ∈ { 0,…,15 }?
    create macro is_connectors(x) as x between 0 and 15;

    -- Map character `c` to the corresponding connector number
    create macro char_to_connectors(c) as 
      case c
        when '█' then 0
        when '╴' then 1
        when '╷' then 2
        when '┐' then 3
        when '╶' then 4
        when '─' then 5
        when '┌' then 6
        when '┬' then 7
        when '╵' then 8
        when '┘' then 9
        when '│' then 10
        when '┤' then 11
        when '└' then 12
        when '┴' then 13
        when '├' then 14
        when '┼' then 15
      end;
    
    create sequence object_seq_id;
    create table objects (
      id         int primary key default nextval('object_seq_id'),
      shape      int not null,
      connectors int not null check (is_connectors(connectors)),
      x          int not null check (is_inbound(x)),
      y          int not null check (is_inbound(y))
    );

    create table undo (
      turn       int not null,
      object_id  int not null,
      shape      int not null,
      connectors int not null check (is_connectors(connectors)),
      x          int not null check (is_inbound(x)),
      y          int not null check (is_inbound(y)),
      primary key(turn, object_id)
    );

    create table redo (
      turn       int not null,
      object_id  int not null,
      shape      int not null,
      connectors int not null check (is_connectors(connectors)),
      x          int not null check (is_inbound(x)),
      y          int not null check (is_inbound(y)),
      primary key(turn, object_id)
    )
    "#)
  }

  fn load_level(&self, level_string: String) -> error::IOResult {
    self.db.execute(r"
      insert into objects(shape,connectors,x,y) 
        select  row_number() over() as shape, char_to_connectors(chars[x]), x, y
        from    (select string_split_regex(?1,'(\r\n|[\r\n])') as rows),
        lateral (select generate_subscripts(rows,1)            as y),
        lateral (select string_split_regex(rows[y],'')         as chars),
        lateral (select generate_subscripts(chars,1)           as x)
        where chars[x] <> ' ';
    ", params![level_string])?;
    Ok(())
  }

  fn populate_database(&self) -> error::IOResult { self.load_level(fs::read_to_string("levels/01-simple.lvl")?) }

  fn shutdown_database(self) -> error::IOResult {
    if let Some((_,e)) = self.db.close().err() { 
      Err(e)?;
    }
    Ok(())
  }

  fn clear_print_all(&self) -> error::IOResult {
    self.state_control_send.send(StateControlPayload::ClearTerminal)?;
    if let Some(selected_shape) = self.selected_shape {
      self.state_control_send.send(StateControlPayload::PrintObjects(
        State::query_objects_via_statement(
          self.db.prepare("select o.id, o.shape, o.connectors, o.x, o.y from objects as o")?, 
          params![],
          |row| { 
            let shape: i32 = row.get(1)?; 
            Ok(Object::new_with_color(row.get(0)?, row.get(1)?, row.get(2)?, (row.get(3)?,row.get(4)?), Some(if selected_shape == shape { Color::White } else { Color::DarkGrey }))) 
          })?
      ))?;
    } else {
      self.state_control_send.send(StateControlPayload::PrintObjects(
        State::query_objects_via_statement(
          self.db.prepare("select o.id, o.shape, o.connectors, o.x, o.y from objects as o")?, 
          params![],
          |row| { 
            Ok(Object::new_with_color(row.get(0)?, row.get(1)?, row.get(2)?, (row.get(3)?,row.get(4)?), Some(Color::DarkGrey))) 
          })?
      ))?;
    }
    Ok(())
  }

  #[inline]
  fn move_cursor_to(pos: &(u16,u16), direction: Direction, (w,h): (u16,u16), shape_selected: bool) -> Option<(u16,u16)> {
    if let (Some(x),Some(y)) = match (direction, shape_selected) {
      (Direction::UpLeft   , false) => (pos.0.checked_sub(1)                                                 , pos.1.checked_sub(1)                                                 ),
      (Direction::Up       , _    ) => (Some(pos.0)                                                          , pos.1.checked_sub(1)                                                 ),
      (Direction::UpRight  , false) => (pos.0.checked_add(1).and_then(|x| if x < w { Some(x) } else { None }), pos.1.checked_sub(1)                                                 ),
      (Direction::Right    , _    ) => (pos.0.checked_add(1).and_then(|x| if x < w { Some(x) } else { None }), Some(pos.1)                                                          ),
      (Direction::DownRight, false) => (pos.0.checked_add(1).and_then(|x| if x < w { Some(x) } else { None }), pos.1.checked_add(1).and_then(|y| if y < h { Some(y) } else { None })),
      (Direction::Down     , _    ) => (Some(pos.0)                                                          , pos.1.checked_add(1).and_then(|y| if y < h { Some(y) } else { None })),
      (Direction::DownLeft , false) => (pos.0.checked_sub(1)                                                 , pos.1.checked_add(1).and_then(|y| if y < h { Some(y) } else { None })),
      (Direction::Left     , _    ) => (pos.0.checked_sub(1)                                                 , Some(pos.1)                                                          ),
      _                             => (None                                                                 , None                                                                 )
    } { Some((x,y)) } else { None }
  }
  fn object_by_pos(&self, (x,y): (u16,u16)) -> duckdb::Result<Option<Object>> { 
    self.db.query_row(r#"
      select o.id, o.shape, o.connectors,
      from   objects as o
      where  (o.x,o.y) = (?1,?2)
      order by o.id
    "#, params![x,y], |row| Ok(Object::new(row.get(0)?, row.get(1)?, row.get(2)?, (x,y)))).optional() 
  }
  fn query_objects_via_statement<T,F>(mut statement: Statement, params: &[&dyn duckdb::ToSql], f :F) -> duckdb::Result<Vec<T>> 
  where 
      F: FnMut(&duckdb::Row<'_>) -> duckdb::Result<T> {
    let mut err = None;
    let objects: Vec<T> = statement
      .query_map(params, f)?
      .map_while(|res| match res { Ok(obj) => Some(obj), Err(e) => { err = Some(e); None } })
      .collect();
    if let Some(e) = err { Err(e) } else { Ok(objects) }
  }
  fn objects_by_shape_with_color(&self, shape: i32, color: Option<Color>) -> duckdb::Result<Vec<Object>> {
    State::query_objects_via_statement(
      self.db.prepare("select o.id, o.shape, o.connectors, o.x, o.y from objects as o where o.shape = ?1 order by o.id")?, 
      params![shape], 
      |row| Ok(Object::new_with_color(row.get(0)?, row.get(1)?, row.get(2)?, (row.get(3)?,row.get(4)?), color))
    )
  }
  fn objects_by_shape_via_tx_with_color(tx: &duckdb::Transaction, shape: i32, color: Option<Color>) -> duckdb::Result<Vec<Object>> {
    State::query_objects_via_statement(tx.prepare("select o.id, o.shape, o.connectors, o.x, o.y from objects as o where o.shape = ?1 order by o.id")?, 
    params![shape], 
    |row| Ok(Object::new_with_color(row.get(0)?, row.get(1)?, row.get(2)?, (row.get(3)?,row.get(4)?), color)))
  }
  fn objects_by_shape_via_tx(tx: &duckdb::Transaction, shape: i32) -> duckdb::Result<Vec<Object>> {
    State::objects_by_shape_via_tx_with_color(tx, shape, None)
  }
  /// Move a `shape` by `(Δx,Δy)` in transaction `tx`
  #[allow(non_snake_case)]
  fn move_shape(tx: &duckdb::Transaction, shape: i32, (Δx,Δy): (i32,i32), (w,h): (u16,u16)) -> MoveObjectResult {
    let here_shape = State::objects_by_shape_via_tx(tx, shape)?;
    if !here_shape.is_empty() {
      // Collision detection
      let is_valid_move: bool = tx.query_row(r#"
        select bool_and(is_inbound(o.x+?1) and is_inbound(o.y+?2)
          and o.x+?1 < ?4 and o.y+?2 < ?5
          and not exists (select 1 
                          from   objects as _o 
                          where  _o.shape <> ?3 
                          and    (_o.x,_o.y) = (o.x+?1,o.y+?2)))
        from   objects as o
        where  o.shape = ?3
      "#, params![Δx,Δy,shape,w,h], |row| row.get(0))?;
      if is_valid_move {
        // Insert current state into undo
        tx.execute_batch(r#"
          insert into undo 
            select coalesce((select max(u.turn)+1 from undo as u), 1), o.*
            from   objects as o;

          delete from redo;
        "#)?;
        // Truncate all turns older than UNDO_SIZE_IN_TURNS
        tx.execute(r#"
          delete from undo
          where  (select max(u.turn) from undo as u) - turn >= ?1
        "#, params![UNDO_SIZE_IN_TURNS])?;
        // Move shape
        tx.execute(r#"
          update objects 
            set   x = x+?1, y = y+?2
            where shape = ?3
        "#, params![Δx,Δy,shape])?;
        if tx.query_row(r#"select 1 from objects as o where o.shape = ?1 limit 1"#, params![shape], |row| row.get(0)).optional().is_ok_and(|o_row: Option<i32>| o_row.is_some()) {
          // Merge shapes, if any are adjacent
          tx.execute(r#"
            update objects
              set   shape = ?1
              where shape <> ?1
              and   shape in (select o.shape 
                              from   objects as o -- Potential merge candidates
                              where  o.shape <> ?1 
                              and    exists (select 1
                                             from   objects as _o -- Selected shape
                                             where  _o.shape = ?1
                                             --     Is the selected shape vert./horiz. adjacent to a potential merge candidate?
                                             and    (((_o.connectors & 8) = 8 and (o.connectors & 2) = 2 and (_o.x,_o.y) = (o.x  ,o.y+1))    -- Selected Up    Connector + Potential Down  Connector
                                             or      ((_o.connectors & 4) = 4 and (o.connectors & 1) = 1 and (_o.x,_o.y) = (o.x-1,o.y  ))    -- Selected Right Connector + Potential Left  Connector
                                             or      ((_o.connectors & 2) = 2 and (o.connectors & 8) = 8 and (_o.x,_o.y) = (o.x  ,o.y-1))    -- Selected Down  Connector + Potential Up    Connector
                                             or      ((_o.connectors & 1) = 1 and (o.connectors & 4) = 4 and (_o.x,_o.y) = (o.x+1,o.y  ))))) -- Selected Left  Connector + Potential Right Connector 
          "#, params![shape])?;
          return Ok(Some((here_shape, State::objects_by_shape_via_tx_with_color(tx, shape, Some(Color::White))?)));
        }
      }
    } 
    Ok(None)
  }

  /// Move cursor in a `direction` and notify the updated position to the controller, if the cursor moved to a new position
  /// If the cursor has an object id selected, move the object with the object id as well, then notify the controller 
  fn move_cursor(&mut self, direction: Direction) -> error::IOResult {
    let cursor_here@(here_x,here_y)    = self.cursor_position();
    if let Some(cursor_there@(there_x,there_y)) = State::move_cursor_to(&cursor_here, direction, self.board_size, self.selected_shape.is_some()) {
      let mut do_move = cursor_here != cursor_there;
      let mut db      = self.db.try_clone()?;
      let tx          = db.transaction()?;
      if do_move {
        if let Some(shape) = self.selected_shape {
          match State::move_shape(&tx, shape, (i32::from(there_x)-i32::from(here_x),i32::from(there_y)-i32::from(here_y)), self.board_size) {
            Ok(None)                            => { do_move = false },
            Ok(Some((here_shape, there_shape))) => {
              do_move = here_shape.len() != there_shape.len() || here_shape.iter().enumerate().any(|(i,here)| here.pos != there_shape[i].pos);
              if do_move {
                self.state_control_send.send(StateControlPayload::MoveShape(here_shape,there_shape))?; // Note: An object always moves before the cursor.
              } 
            },
            Err(e) => {
              log::error!("State: Tried moving a selected shape ({}), but it failed: {}", shape, e); 
              return Err(e)
            },
          }
        } 
        if do_move {
          self.cursor_pos = cursor_there;
          self.state_control_send.send(StateControlPayload::SetCursorPosition((self.cursor_pos.0, self.cursor_pos.1)))?;
        }
      }
      tx.commit()?;
    }
    Ok(())
  }

  fn set_cursor_position(&mut self, pos: (u16,u16)) -> error::IOResult {
    if self.selected_shape.is_some() {
      self.toggle_select_shape()?;
    }
    self.cursor_pos = pos;
    self.state_control_send.send(StateControlPayload::SetCursorPosition((self.cursor_pos.0, self.cursor_pos.1)))?;
    Ok(())
  }

  /// If no shape is selected, select the one at the cursor position, if any
  /// If an shape is selected, deselect the shape
  fn toggle_select_shape(&mut self) -> error::IOResult { 
    if let Some(shape) = self.selected_shape { 
      self.state_control_send.send(StateControlPayload::PrintObjects(self.objects_by_shape_with_color(shape, Some(Color::DarkGrey))?))?;
      self.selected_shape = None; 
    } else { 
      self.selected_shape = self.object_by_pos(self.cursor_position())?.filter(|obj| obj.connectors != 0).map(|obj| obj.shape); 
    } 
    if let Some(shape) = self.selected_shape {
      self.state_control_send.send(StateControlPayload::PrintObjects(self.objects_by_shape_with_color(shape, Some(Color::White))?))?;
    }
    Ok(()) 
  }

  fn set_board_size(&mut self, size@(w,h): (u16,u16)) -> error::IOResult {
    let old_board_size = self.board_size;
    self.board_size = 
    if let Some((min_w, min_h)) = self.db.query_row(r#"
      select max(o.x), max(o.y) from objects as o
      "#, params![], |row| Ok((row.get(0)?, row.get(1)?))).optional()? {
        (max(min_w, w), max(min_h, h))
      } else { size };
    if old_board_size != self.board_size {
      self.state_control_send.send(StateControlPayload::ResizeTerminal(self.board_size))?;
      self.clear_print_all()?;
    }
    Ok(())
  }

  fn undo(&mut self) -> error::IOResult {
    // If there are any turns to undo, undo it
    if self.db.query_row("select exists (select * from undo as u)", params![], |row| row.get(0))? {
      let mut db = self.db.try_clone()?;
      let tx = db.transaction()?;
      tx.execute_batch(r#"
        insert into redo
          select (select max(u.turn) from undo u)+1, o.* from objects as o;

        insert or replace into objects(id,shape,connectors,x,y)
          select columns(* exclude (turn)) from undo as u where u.turn = (select max(u.turn) from undo u);

        delete from undo
          where turn = (select max(u.turn) from undo u);
      "#)?;
      tx.commit()?;
      self.selected_shape = None;
      self.clear_print_all()?;
    }
    Ok(())
  }

  fn redo(&mut self) -> error::IOResult {
    // If there are any turns to undo, undo it
    if self.db.query_row("select exists (select * from redo as r)", params![], |row| row.get(0))? {
      let mut db = self.db.try_clone()?;
      let tx = db.transaction()?;
      tx.execute_batch(r#"
        insert into undo
        select (select min(r.turn) from redo r)-1, o.* from objects as o;
          
        insert or replace into objects(id,shape,connectors,x,y)
          select columns(* exclude (turn)) from redo as r where r.turn = (select min(r.turn) from redo r);

        delete from redo
          where turn = (select min(r.turn) from redo r);
      "#)?;
      tx.commit()?;
      self.selected_shape = None;
      self.clear_print_all()?;
    }
    Ok(())
  }

  #[inline]
  fn get_save_file_path(&self) -> String { format!("{}.sav", self.level_path.clone().map_or(SAVE_FILE_PATH.to_string(), |s| s.replace(".lvl", ""))) }

  fn save(&self) -> error::IOResult {
    let save_file_path = self.get_save_file_path();
    if Path::new(TEMP_SAVE_PATH).exists() {
      fs::remove_dir_all(TEMP_SAVE_PATH)?;
    }
    fs::create_dir(TEMP_SAVE_PATH)?;
    self.db.execute(r#"export database 'temp-save' (encoding utf8)"#, params![])?;

    let mut archiver = Archiver::new();
    archiver.push(TEMP_SAVE_PATH);
    archiver.set_destination(Path::new(&".".to_string()));
    if let Err(e) = archiver.archive() {
      log::error!("Save failed: {}", e);
    } else {
      if Path::new(&save_file_path).exists() {
        fs::remove_file(&save_file_path)?;
      }
      fs::rename(
        Path::new(&format!("{}.zip", TEMP_SAVE_PATH)), 
        Path::new(&save_file_path)
      )?;
    }
    if Path::new(TEMP_SAVE_PATH).exists() {
      fs::remove_dir_all(TEMP_SAVE_PATH)?;
    }
    Ok(())
  }

  fn load(&mut self) -> error::IOResult {
    let save_file_path = self.get_save_file_path();
    if let Err(e) = zip_extract::extract(fs::File::open(Path::new(&save_file_path))?, Path::new("."), false) {
      log::error!("Load {} failed: {}", &save_file_path, e);
    } else {
      self.db = Connection::open_in_memory()?;
      self.init_database()?;
      self.db.execute_batch(fs::read_to_string(Path::new(format!("{}/load.sql", TEMP_SAVE_PATH).as_str()))?.as_str())?;
    }
    if Path::new(TEMP_SAVE_PATH).exists() {
      fs::remove_dir_all(TEMP_SAVE_PATH)?;
    }
    self.selected_shape = None;
    self.clear_print_all()?;
    Ok(())
  }
  
  /// Starts the main loop
  pub fn maintain(mut self) -> error::IOResult {
    let mut now = time::Instant::now();
    self.init_database()?;
    self.populate_database()?;
    self.state_control_send.send(StateControlPayload::SetCursorPosition((self.cursor_pos.0, self.cursor_pos.1)))?;
    self.clear_print_all()?;
    loop {
      // Make sure, we wait to keep input rate consistent
      common::wait_minus_elapsed(time::Duration::from_millis(SENDING_RATE_IN_MSECS), now.elapsed());
      now = time::Instant::now();

      match self.control_state_recv.recv()? {
        ControlStatePayload::MoveCursor(direction)    => self.move_cursor(direction)?,
        ControlStatePayload::SetCursorPosition(pos)   => self.set_cursor_position(pos)?,
        ControlStatePayload::Select                   => self.toggle_select_shape()?,
        ControlStatePayload::SetBoardSize(board_size) => self.set_board_size(board_size)?,
        ControlStatePayload::Undo                     => self.undo()?,
        ControlStatePayload::Redo                     => self.redo()?,
        ControlStatePayload::Save                     => self.save()?,
        ControlStatePayload::Load                     => self.load()?,
        ControlStatePayload::Shutdown                 => break
      }
    }
    self.shutdown_database()?;
    Ok(())
  }
}

#[cfg(test)]
mod tests {
    use core::panic;
    use std::thread;

    use super::*;

    const WAIT_FOR_DUMMY_THREAD_IN_SECS: u64   = 3;

    // Cursor movement tests
    #[test] fn move_cursor_up()         { assert_eq!(State::move_cursor_to(&(1,1), Direction::Up,        (u16::MAX,u16::MAX), false), Some((1+0,1-1))); }
    #[test] fn move_cursor_up_right()   { assert_eq!(State::move_cursor_to(&(1,1), Direction::UpRight,   (u16::MAX,u16::MAX), false), Some((1+1,1-1))); }
    #[test] fn move_cursor_right()      { assert_eq!(State::move_cursor_to(&(1,1), Direction::Right,     (u16::MAX,u16::MAX), false), Some((1+1,1+0))); }
    #[test] fn move_cursor_down_right() { assert_eq!(State::move_cursor_to(&(1,1), Direction::DownRight, (u16::MAX,u16::MAX), false), Some((1+1,1+1))); }
    #[test] fn move_cursor_down()       { assert_eq!(State::move_cursor_to(&(1,1), Direction::Down,      (u16::MAX,u16::MAX), false), Some((1+0,1+1))); }
    #[test] fn move_cursor_down_left()  { assert_eq!(State::move_cursor_to(&(1,1), Direction::DownLeft,  (u16::MAX,u16::MAX), false), Some((1-1,1+1))); }
    #[test] fn move_cursor_left()       { assert_eq!(State::move_cursor_to(&(1,1), Direction::Left,      (u16::MAX,u16::MAX), false), Some((1-1,1+0))); }
    #[test] fn move_cursor_up_left()    { assert_eq!(State::move_cursor_to(&(1,1), Direction::UpLeft,    (u16::MAX,u16::MAX), false), Some((1-1,1-1))); }

    #[test] 
    // Cursor movement tests that try to go over the limits
    fn no_move_cases() {
      assert_eq!(State::move_cursor_to(&(u16::MAX-1,0         ), Direction::Up,        (u16::MAX, u16::MAX), false), None);
      assert_eq!(State::move_cursor_to(&(u16::MAX-1,0         ), Direction::UpRight,   (u16::MAX, u16::MAX), false), None);
      assert_eq!(State::move_cursor_to(&(u16::MAX-1,0         ), Direction::Right,     (u16::MAX, u16::MAX), false), None);
      assert_eq!(State::move_cursor_to(&(u16::MAX-1,u16::MAX-1), Direction::DownRight, (u16::MAX, u16::MAX), false), None);
      assert_eq!(State::move_cursor_to(&(0         ,u16::MAX-1), Direction::Down,      (u16::MAX, u16::MAX), false), None);
      assert_eq!(State::move_cursor_to(&(0         ,u16::MAX-1), Direction::DownLeft,  (u16::MAX, u16::MAX), false), None);
      assert_eq!(State::move_cursor_to(&(0         ,u16::MAX-1), Direction::Left,      (u16::MAX, u16::MAX), false), None);
      assert_eq!(State::move_cursor_to(&(0         ,0         ), Direction::UpLeft,    (u16::MAX, u16::MAX), false), None);
    }

    #[test] 
    // Initialize and populate the database and see, if the expected number of objects are created
    fn initalize_and_populate_database() -> error::IOResult {
      let (state, _, _) = State::new()?;
      state.init_database()?;
      state.populate_database()?;
      Ok(())
    }

    #[test]
    // Make sure the initial cursor position is as expected
    fn initital_cursor_position() -> error::IOResult {
      let (state, _, _) = State::new()?;
      assert_eq!(state.cursor_position(), (INITIAL_CURSOR_POS_X, INITIAL_CURSOR_POS_Y));
      Ok(())
    }

    // Helper function that adds an object to the initialized database of state
    fn add_object(state: &State, shape: i32, conectors: i32, x: u16, y: u16) -> duckdb::Result<usize> { state.db.execute(r#"insert into objects(shape,connectors,x,y) select ?1,?2,?3,?4"#, params![shape,conectors,x,y]) }

    // Wrapper function to query an object by shape via transaction
    fn object_by_id(state: &State, shape: i32) -> duckdb::Result<Option<Object>> { 
      state.db.query_row(r#"
        select o.id, o.shape, o.connectors, o.x, o.y 
        from   objects as o 
        where  o.shape = ?1
      "#, params![shape], |row| Ok(Object::new(row.get(0)?, row.get(1)?, row.get(2)?, (row.get(3)?, row.get(4)?)))).optional() }

    #[test] 
    // Populate the database with one object {id: 1, shape: 1, pos: (2,3)} and test object_by_id and object_by_pos
    fn object_by_id_and_pos_database() -> error::IOResult {
      const SHAPE: i32      = 1;
      const CONNECTORS: i32 = 15;
      const X: u16          = 2; 
      const Y: u16          = 3;
      let (state, _, _) = State::new()?;
      state.init_database()?;
      assert_eq!(add_object(&state, SHAPE, CONNECTORS, X, Y)?, 1);
      assert_eq!(object_by_id(&state,1)?, Some(Object::new(1, SHAPE, CONNECTORS, (X,Y))));
      assert_eq!(state.object_by_pos((X,Y))?, Some(Object::new(1, SHAPE, CONNECTORS, (X,Y))));
      Ok(())
    }

    #[test]
    // Move the cursor and select empty space until you read an object {id: 1, shape: 1, pos: (2,3)}, 
    // select it, then move it down one and then deselect it and move the cursor some more
    fn move_cursor_and_objects() -> error::IOResult {
      const SHAPE: i32      = 1;
      const CONNECTORS: i32 = 15;
      const X: u16          = 2; 
      const Y: u16          = 3;
      let (mut state, _, dummy_recv) = State::new()?;

      let dummy_thread = thread::spawn(move || {
        fn assert_received_cursor_position(dummy_recv: &Receiver<StateControlPayload>, expected_pos: (u16,u16)) {
          if let Ok(StateControlPayload::SetCursorPosition(actual_pos)) = dummy_recv.recv() { 
            assert_eq!(actual_pos, expected_pos)
          } else { 
            panic!("Did not receive expected payload") 
          }
        }
        fn assert_received_object_movement(dummy_recv: &Receiver<StateControlPayload>, expected_here: Object, expected_there: Object) {
          if let Ok(StateControlPayload::MoveShape(here_shape,there_shape)) = dummy_recv.recv() { 
            assert_eq!(here_shape.len(), 1);
            assert_eq!(there_shape.len(), 1);
            assert_eq!((here_shape[0], there_shape[0]), (expected_here, expected_there))
          } else { 
            panic!("Did not receive expected payload") 
          }
        }
        fn assert_received_print_objects(dummy_recv: &Receiver<StateControlPayload>, expected_obj: Object) {
          if let Ok(StateControlPayload::PrintObjects(shape)) = dummy_recv.recv() { 
            assert_eq!(shape.len(), 1);
            assert_eq!(shape[0], expected_obj)
          } else { 
            panic!("Did not receive expected payload") 
          }
        }
        assert_received_cursor_position(&dummy_recv, (INITIAL_CURSOR_POS_X+1,INITIAL_CURSOR_POS_Y));
        assert_received_cursor_position(&dummy_recv, (INITIAL_CURSOR_POS_X+2,INITIAL_CURSOR_POS_Y));
        assert_received_cursor_position(&dummy_recv, (INITIAL_CURSOR_POS_X+2,INITIAL_CURSOR_POS_Y+1));
        assert_received_cursor_position(&dummy_recv, (INITIAL_CURSOR_POS_X+2,INITIAL_CURSOR_POS_Y+2));
        assert_received_cursor_position(&dummy_recv, (INITIAL_CURSOR_POS_X+2,INITIAL_CURSOR_POS_Y+3));
          assert_received_print_objects(&dummy_recv, Object::new_with_color(1, SHAPE, CONNECTORS, (X,Y), Some(Color::White)));
        assert_received_object_movement(&dummy_recv, Object::new_with_color(1, SHAPE, CONNECTORS, (X,Y), None), Object::new_with_color(1, SHAPE, CONNECTORS, (X,Y+1), Some(Color::White)));
        assert_received_cursor_position(&dummy_recv, (INITIAL_CURSOR_POS_X+2,INITIAL_CURSOR_POS_Y+4));
        assert_received_object_movement(&dummy_recv, Object::new(1, SHAPE, CONNECTORS, (X,Y+1)), Object::new_with_color(1, SHAPE, CONNECTORS, (X-1,Y+1), Some(Color::White)));
        assert_received_cursor_position(&dummy_recv, (INITIAL_CURSOR_POS_X+1,INITIAL_CURSOR_POS_Y+4));
        assert_received_object_movement(&dummy_recv, Object::new(1, SHAPE, CONNECTORS, (X-1,Y+1)), Object::new_with_color(1, SHAPE, CONNECTORS, (X,Y+1), Some(Color::White)));
        assert_received_cursor_position(&dummy_recv, (INITIAL_CURSOR_POS_X+2,INITIAL_CURSOR_POS_Y+4));
        assert_received_object_movement(&dummy_recv, Object::new(1, SHAPE, CONNECTORS, (X,Y+1)), Object::new_with_color(1, SHAPE, CONNECTORS, (X,Y), Some(Color::White)));
        assert_received_cursor_position(&dummy_recv, (INITIAL_CURSOR_POS_X+2,INITIAL_CURSOR_POS_Y+3));
        assert_received_object_movement(&dummy_recv, Object::new(1, SHAPE, CONNECTORS, (X,Y)), Object::new_with_color(1, SHAPE, CONNECTORS, (X,Y+1), Some(Color::White)));
        assert_received_cursor_position(&dummy_recv, (INITIAL_CURSOR_POS_X+2,INITIAL_CURSOR_POS_Y+4));
          assert_received_print_objects(&dummy_recv, Object::new_with_color(1, SHAPE, CONNECTORS, (X,Y+1), Some(Color::DarkGrey)));
        assert_received_cursor_position(&dummy_recv, (INITIAL_CURSOR_POS_X+3,INITIAL_CURSOR_POS_Y+4));
      });

      state.init_database()?;
      assert_eq!(add_object(&state, SHAPE, CONNECTORS, X, Y)?, 1);
      assert_eq!(state.cursor_position(), (INITIAL_CURSOR_POS_X, INITIAL_CURSOR_POS_Y));
      assert_eq!(state.selected_shape, None);
      state.toggle_select_shape()?;
      assert_eq!(state.cursor_position(), (INITIAL_CURSOR_POS_X, INITIAL_CURSOR_POS_Y));
      assert_eq!(state.selected_shape, None);
      state.move_cursor(Direction::Right)?;
      state.toggle_select_shape()?;
      assert_eq!(state.cursor_position(), (INITIAL_CURSOR_POS_X+1, INITIAL_CURSOR_POS_Y));
      assert_eq!(state.selected_shape, None);
      state.move_cursor(Direction::Right)?;
      state.move_cursor(Direction::Down)?;
      state.move_cursor(Direction::Down)?;
      state.move_cursor(Direction::Down)?;
      state.toggle_select_shape()?;
      assert_eq!(state.cursor_position(), (INITIAL_CURSOR_POS_X+2, INITIAL_CURSOR_POS_Y+3));
      assert_eq!(object_by_id(&state,1)?, Some(Object::new(1, SHAPE, CONNECTORS, (X,Y))));
      assert_eq!(state.selected_shape, Some(1));
      state.move_cursor(Direction::Down)?;
      assert_eq!(state.cursor_position(), (INITIAL_CURSOR_POS_X+2, INITIAL_CURSOR_POS_Y+4));
      assert_eq!(object_by_id(&state,1)?, Some(Object::new(1, SHAPE, CONNECTORS, (X,Y+1))));
      assert_eq!(state.selected_shape, Some(1));
      state.move_cursor(Direction::Left)?;
      assert_eq!(state.cursor_position(), (INITIAL_CURSOR_POS_X+1, INITIAL_CURSOR_POS_Y+4));
      assert_eq!(object_by_id(&state,1)?, Some(Object::new(1, SHAPE, CONNECTORS, (X-1,Y+1))));
      assert_eq!(state.selected_shape, Some(1));
      state.move_cursor(Direction::Right)?;
      assert_eq!(state.cursor_position(), (INITIAL_CURSOR_POS_X+2, INITIAL_CURSOR_POS_Y+4));
      assert_eq!(object_by_id(&state,1)?, Some(Object::new(1, SHAPE, CONNECTORS, (X,Y+1))));
      assert_eq!(state.selected_shape, Some(1));
      state.move_cursor(Direction::Up)?;
      assert_eq!(state.cursor_position(), (INITIAL_CURSOR_POS_X+2, INITIAL_CURSOR_POS_Y+3));
      assert_eq!(object_by_id(&state,1)?, Some(Object::new(1, SHAPE, CONNECTORS, (X,Y))));
      assert_eq!(state.selected_shape, Some(1));
      state.move_cursor(Direction::Down)?;
      assert_eq!(state.cursor_position(), (INITIAL_CURSOR_POS_X+2, INITIAL_CURSOR_POS_Y+4));
      assert_eq!(object_by_id(&state,1)?, Some(Object::new(1, SHAPE, CONNECTORS, (X,Y+1))));
      assert_eq!(state.selected_shape, Some(1));
      state.toggle_select_shape()?;
      assert_eq!(state.cursor_position(), (INITIAL_CURSOR_POS_X+2, INITIAL_CURSOR_POS_Y+4));
      assert_eq!(object_by_id(&state,1)?, Some(Object::new(1, SHAPE, CONNECTORS, (X,Y+1))));
      assert_eq!(state.selected_shape, None);
      state.move_cursor(Direction::Right)?;
      assert_eq!(state.cursor_position(), (INITIAL_CURSOR_POS_X+3, INITIAL_CURSOR_POS_Y+4));
      assert_eq!(object_by_id(&state,1)?, Some(Object::new(1, SHAPE, CONNECTORS, (X,Y+1))));
      assert_eq!(state.selected_shape, None);
      if !dummy_thread.is_finished() { thread::sleep(time::Duration::from_secs(WAIT_FOR_DUMMY_THREAD_IN_SECS)); if !dummy_thread.is_finished() { panic!("Thread did not finish after waiting for it for at least {} seconds", WAIT_FOR_DUMMY_THREAD_IN_SECS) } }
      Ok(())
    }
}