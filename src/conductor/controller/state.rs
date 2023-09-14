use std::{sync::mpsc::{Sender, SyncSender, Receiver, self}, time, cmp::max, fs, path::Path};
use crossterm::style::Color;
use duckdb::{Connection, params, OptionalExt, Statement};
use zip_archive::Archiver;

use crate::common;

use super::error;

type MoveObjectResult = Result<Option<(Vec<Object>,Vec<Object>,Option<i32>)>, error::IOError>;

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

#[derive(Debug, PartialEq, Eq)]
pub enum StateControlPayload { ClearTerminal, PrintObjects(Vec<Object>), SetCursorPosition((u16,u16)), MoveShape(Vec<Object>,Vec<Object>), ResizeTerminal((u16,u16)), TurnCounter(u16,i32,bool) }

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Object { id: i32, shape: i32, color: Option<Color>, connectors: i32, kind: String, pos: (u16,u16) }

impl Object {
  fn new(id: i32, shape: i32, connectors: i32, kind: String, pos: (u16,u16)) -> Self { Object { id, shape, connectors, kind, pos, color: None } }
  fn new_with_color(id: i32, shape: i32, connectors: i32, kind: String, pos: (u16,u16), color: Option<Color>) -> Self { Object { id, shape, connectors, kind, pos, color } }
  pub fn connectors(&self) -> i32 { self.connectors }
  pub fn pos(&self) -> (u16,u16) { self.pos }
  pub fn color(&self) -> Option<Color> { self.color }
  pub fn kind(&self) -> String { self.kind.clone() }
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
    -- Object type enum
    create type kind as enum ('None','Wide','Door','Volatile');

    -- Is `x` ∈ { 0,…,65534 }?
    create macro is_inbound(x) as x between 0 and 65534;

    -- Is `x` ∈ { 0,…,15 }?
    create macro is_connectors(x) as x between 0 and 240;

    -- Map character `c` to the corresponding connector number
    create macro char_to_connectors(c) as
      case
        when c in ('█','◊') then   0
        when c =   '╴'      then   1
        when c =   '╸'      then ( 1 << 4)
        when c =   '╷'      then   2
        when c =   '╻'      then ( 2 << 4)
        when c =   '┐'      then   3
        when c in ('┒','╖') then ( 2 << 4) + 1
        when c in ('┑','╕') then ( 1 << 4) + 2
        when c in ('┓','╗') then ( 3 << 4)
        when c =   '╶'      then   4
        when c =   '╺'      then ( 4 << 4)
        when c =   '─'      then   5
        when c =   '╼'      then ( 4 << 4) + 1
        when c =   '╾'      then ( 1 << 4) + 4
        when c in ('━','═') then ( 5 << 4)
        when c =   '┌'      then   6
        when c in ('┍','╒') then ( 4 << 4) + 2
        when c in ('┎','╓') then ( 2 << 4) + 4
        when c in ('┏','╔') then ( 6 << 4)
        when c =   '┬'      then   7
        when c =   '┮'      then ( 4 << 4) + 3
        when c =   '┭'      then ( 1 << 4) + 6
        when c =   '┲'      then ( 6 << 4) + 1
        when c =   '┱'      then ( 3 << 4) + 4
        when c in ('┰','╥') then ( 2 << 4) + 5
        when c in ('┯','╤') then ( 5 << 4) + 2
        when c in ('┳','╦') then ( 7 << 4)
        when c =   '╵'      then   8
        when c =   '╹'      then ( 8 << 4)
        when c =   '┘'      then   9
        when c in ('┚','╜') then ( 8 << 4) + 1
        when c in ('┙','╛') then ( 1 << 4) + 8
        when c in ('┛','╝') then ( 9 << 4)
        when c =   '│'      then  10
        when c =   '╿'      then ( 8 << 4) + 2
        when c =   '╽'      then ( 2 << 4) + 8
        when c in ('┃','║') then (10 << 4)
        when c =   '┤'      then  11
        when c =   '┦'      then ( 8 << 4) + 3
        when c =   '┧'      then ( 2 << 4) + 9
        when c =   '┩'      then ( 9 << 4) + 2
        when c =   '┪'      then ( 3 << 4) + 8
        when c in ('┥','╡') then ( 1 << 4) + 10
        when c in ('┨','╢') then (10 << 4) + 1
        when c in ('┫','╣') then (11 << 4)
        when c =   '└'      then  12
        when c in ('┖','╙') then ( 8 << 4) + 4
        when c in ('┕','╘') then ( 4 << 4) + 8
        when c in ('┗','╚') then (12 << 4)
        when c =   '┴'      then  13
        when c =   '┶'      then ( 4 << 4) + 9
        when c =   '┵'      then ( 1 << 4) + 12
        when c =   '┺'      then (12 << 4) + 1
        when c =   '┹'      then ( 9 << 4) + 4
        when c in ('┸','╨') then ( 8 << 4) + 5
        when c in ('┷','╧') then ( 5 << 4) + 8
        when c in ('┻','╩') then (13 << 4)
        when c =   '├'      then  14
        when c =   '┞'      then ( 8 << 4) + 6
        when c =   '┟'      then ( 2 << 4) + 12
        when c =   '┡'      then (12 << 4) + 2
        when c =   '┢'      then ( 6 << 4) + 8
        when c in ('┝','╞') then ( 4 << 4) + 9
        when c in ('┠','╟') then ( 9 << 4) + 4
        when c in ('┣','╠') then (14 << 4)
        when c =   '┼'      then  15
        when c =   '╀'      then ( 8 << 4) + 7
        when c =   '╈'      then ( 7 << 4) + 8
        when c =   '┾'      then ( 4 << 4) + 11
        when c =   '╉'      then (11 << 4) + 4
        when c =   '╁'      then ( 2 << 4) + 13
        when c =   '╇'      then (13 << 4) + 2
        when c =   '┽'      then (14 << 4) + 1
        when c =   '╊'      then ( 1 << 4) + 14
        when c =   '╅'      then ( 3 << 4) + 12
        when c =   '╄'      then (12 << 4) + 3
        when c =   '╃'      then ( 9 << 4) + 6
        when c =   '╆'      then ( 6 << 4) + 9
        when c =  ('╂','╫') then (10 << 4) + 5
        when c =  ('┿','╪') then ( 5 << 4) + 10
        when c in ('╋','╬') then (15 << 4)
      end;

    -- Map character `c` to enum `kind`
    create macro char_to_kind(c) as
      case
        when c = '◊'                                                                                                                    then 'Volatile'
        when c in ('╸','╻','┒','┑','┓','╺','╼','╾','━','┍','┎','┏','┮','┰','┭','┲','┱','┯','┳','╹','┚',
                   '┙','┛','╿','╽','┃','┦','┧','┥','┩','┨','┪','┫','┖','┕','┗','┸','┶','┵','┺','┷','┹',
                   '┻','┞','┝','┟','┡','┢','┠','┣','╀','┾','╁','┽','╄','╂','╃','╆','┿','╅','╊','╇','╉','╈','╋')                         then 'Wide'
        when c in ('╖','╕','╗','═','╒','╓','╔','╥','╤','╦','╜','╛','╝','║','╢','╡','╣','╙','╘','╚','╨','╧','╩','╞','╟','╠','╫','╪','╬') then 'Door'
        else 'None'
      end :: kind;

    -- Returns true, if this and the other object at position (x,y) and (ox,oy) are vertically or horizontally adjacend and both connectors c and oc align and kind k = kind ok
    create macro "connects?"(c,k,x,y,oc,ok,ox,oy) as
      k = ok and ((c & 128) = 128 and (oc &  32) =  32 and (x,y) = (ox  ,oy+1))  -- Selected Up    Special Connector + Potential Down  Special Connector
              or ((c &  64) =  64 and (oc &  16) =  16 and (x,y) = (ox-1,oy  ))  -- Selected Right Special Connector + Potential Left  Special Connector
              or ((c &  32) =  32 and (oc & 128) = 128 and (x,y) = (ox  ,oy-1))  -- Selected Down  Special Connector + Potential Up    Special Connector
              or ((c &  16) =  16 and (oc &  64) =  64 and (x,y) = (ox+1,oy  ))  -- Selected Left  Special Connector + Potential Right Special Connector
              or ((c &   8) =   8 and (oc &   2) =   2 and (x,y) = (ox  ,oy+1))  -- Selected Up    Connector         + Potential Down  Connector
              or ((c &   4) =   4 and (oc &   1) =   1 and (x,y) = (ox-1,oy  ))  -- Selected Right Connector         + Potential Left  Connector
              or ((c &   2) =   2 and (oc &   8) =   8 and (x,y) = (ox  ,oy-1))  -- Selected Down  Connector         + Potential Up    Connector
              or ((c &   1) =   1 and (oc &   4) =   4 and (x,y) = (ox+1,oy  )); -- Selected Left  Connector         + Potential Right Connector

    create sequence object_seq_id;
    create sequence shape_seq_id;
    create table objects (
      id         int  primary key default nextval('object_seq_id'),
      shape      int  not null default nextval('shape_seq_id'),
      connectors int  not null check (is_connectors(connectors)),
      kind       kind not null,
      x          int  not null check (is_inbound(x)),
      y          int  not null check (is_inbound(y))
    );

    -- Returns true, if an object at (ox,oy) with connectors oc and and kind ok is part of a complete shape
    create macro "is complete?"(oc,ok,ox,oy) as
      not (   (oc & 128) = 128 and not exists (select 1 from objects as _o where (_o.connectors &  32) =  32 and ok = _o.kind and (ox,oy) = (_o.x  ,_o.y+1))
           or (oc &  64) =  64 and not exists (select 1 from objects as _o where (_o.connectors &  16) =  16 and ok = _o.kind and (ox,oy) = (_o.x-1,_o.y  ))
           or (oc &  32) =  32 and not exists (select 1 from objects as _o where (_o.connectors & 128) = 128 and ok = _o.kind and (ox,oy) = (_o.x  ,_o.y-1))
           or (oc &  16) =  16 and not exists (select 1 from objects as _o where (_o.connectors &  64) =  64 and ok = _o.kind and (ox,oy) = (_o.x+1,_o.y  ))
           or (oc &   8) =   8 and not exists (select 1 from objects as _o where (_o.connectors &   2) =   2                  and (ox,oy) = (_o.x  ,_o.y+1))
           or (oc &   4) =   4 and not exists (select 1 from objects as _o where (_o.connectors &   1) =   1                  and (ox,oy) = (_o.x-1,_o.y  ))
           or (oc &   2) =   2 and not exists (select 1 from objects as _o where (_o.connectors &   8) =   8                  and (ox,oy) = (_o.x  ,_o.y-1))
           or (oc &   1) =   1 and not exists (select 1 from objects as _o where (_o.connectors &   4) =   4                  and (ox,oy) = (_o.x+1,_o.y  )));

    create table undo (
      turn       int  not null,
      object_id  int  not null,
      shape      int  not null,
      connectors int  not null check (is_connectors(connectors)),
      kind       kind not null,
      x          int  not null check (is_inbound(x)),
      y          int  not null check (is_inbound(y)),
      primary key(turn, object_id)
    );

    create table redo (
      turn       int  not null,
      object_id  int  not null,
      shape      int  not null,
      connectors int  not null check (is_connectors(connectors)),
      kind       kind not null,
      x          int  not null check (is_inbound(x)),
      y          int  not null check (is_inbound(y)),
      primary key(turn, object_id)
    )
    "#)
  }

  fn load_level(&self, level_string: String) -> error::IOResult {
    // Create temporary parsing table
    self.db.execute(r"
      create temporary table parsed_objects (
        connectors int  not null check (is_connectors(connectors)),
        kind       kind not null,
        x          int  not null check (is_inbound(x)),
        y          int  not null check (is_inbound(y))
      );
    ", params![])?;

    // Parse the level string
    self.db.execute(r"
      insert into parsed_objects(connectors,kind,x,y)
        select  char_to_connectors(chars[x]), char_to_kind(chars[x]), x, y
        from    (select string_split_regex(?1,'(\r\n|[\r\n])') as rows),
        lateral (select generate_subscripts(rows,1)            as y),
        lateral (select string_split_regex(rows[y],'')         as chars),
        lateral (select generate_subscripts(chars,1)           as x)
        where chars[x] <> ' ';
    ", params![level_string])?;

    // Add walls
    self.db.execute_batch(r#"
      insert into objects(connectors,kind,x,y)
      select * from parsed_objects as po where po.connectors = 0;

      delete from parsed_objects
      where connectors = 0;
    "#)?;

    // Keep forming objects into shapes until no more shapes are added
    while self.db.execute(r#"
      insert into objects(shape,connectors,kind,x,y)
      with recursive form_shape(shape,connectors,kind,x,y) as (
        (select nextval('shape_seq_id'), po.*
         from   parsed_objects as po
         where  po.connectors > 0
         and    not exists (select 1
                            from   objects as o
                            where  (o.connectors,o.kind,o.x,o.y) = (po.connectors,po.kind,po.x,po.y))
         limit 1)
          union
        select fs.shape, po.*
        from   form_shape as fs, parsed_objects as po
        where  "connects?"(fs.connectors, fs.kind, fs.x, fs.y, po.connectors, po.kind, po.x, po.y)
      )
      select fs.*
      from   form_shape as fs
    "#, params![])? > 0 {
      self.db.execute(r#"
        delete from parsed_objects as po
        where exists (select 1
                      from   objects as o
                      where  (o.connectors,o.kind,o.x,o.y) = (po.connectors,po.kind,po.x,po.y))
      "#, params![])?;
    }

    if !self.db.query_row("select count(*) == 0 from parsed_objects as po", params![], |row| row.get(0))? {
      Err(error::IOError::ParseLevelError)
    } else {
      self.db.execute("drop table parsed_objects", params![])?;
      Ok(())
    }
  }

  fn populate_database(&self) -> error::IOResult { if let Some(level_path) = self.level_path.clone() { self.load_level(fs::read_to_string(level_path)?)?; } Ok(()) }

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
          self.db.prepare("select o.id, o.shape, o.connectors, o.kind::text, o.x, o.y from objects as o")?,
          params![],
          |row| {
            let shape: i32 = row.get(1)?;
            Ok(Object::new_with_color(row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, (row.get(4)?,row.get(5)?), Some(if selected_shape == shape { Color::White } else { Color::DarkGrey })))
          })?
      ))?;
    } else {
      self.state_control_send.send(StateControlPayload::PrintObjects(
        State::query_objects_via_statement(
          self.db.prepare("select o.id, o.shape, o.connectors, o.kind::text, o.x, o.y from objects as o")?,
          params![],
          |row| {
            Ok(Object::new_with_color(row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, (row.get(4)?,row.get(5)?), Some(Color::DarkGrey)))
          })?
      ))?;
    }
    self.print_turn_counter()?;
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
      select o.id, o.shape, o.connectors, o.kind::text
      from   objects as o
      where  (o.x,o.y) = (?1,?2)
      order by o.id
    "#, params![x,y], |row| Ok(Object::new(row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, (x,y)))).optional()
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
      self.db.prepare("select o.id, o.shape, o.connectors, o.kind::text, o.x, o.y from objects as o where o.shape = ?1 order by o.id")?,
      params![shape],
      |row| Ok(Object::new_with_color(row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, (row.get(4)?,row.get(5)?), color))
    )
  }
  fn objects_by_shape_via_tx_with_color(tx: &duckdb::Transaction, shape: i32, color: Option<Color>) -> duckdb::Result<Vec<Object>> {
    State::query_objects_via_statement(tx.prepare("select o.id, o.shape, o.connectors, o.kind::text, o.x, o.y from objects as o where o.shape = ?1 order by o.id")?,
    params![shape],
    |row| Ok(Object::new_with_color(row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, (row.get(4)?,row.get(5)?), color)))
  }
  fn objects_by_shape_via_tx(tx: &duckdb::Transaction, shape: i32) -> duckdb::Result<Vec<Object>> {
    State::objects_by_shape_via_tx_with_color(tx, shape, None)
  }
  /// Move a `shape` by `(Δx,Δy)` in transaction `tx`
  #[allow(non_snake_case)]
  fn move_shape(tx: &duckdb::Transaction, shape: i32, (here_x,here_y): (u16,u16), there@(there_x,there_y): (u16,u16), (w,h): (u16,u16)) -> MoveObjectResult {
    let (Δx,Δy) = (i32::from(there_x)-i32::from(here_x),i32::from(there_y)-i32::from(here_y));
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
            select coalesce((select max(u.turn)+1 from undo as u), (select min(r.turn)-1 from redo as r), 0), o.*
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
          // Merge shapes, if any are adjacent and then do things
          // If number of merged objects is larger than 0, check for doors
          if tx.execute(r#"
            update objects
              set   shape = ?1
              where shape <> ?1
              and   shape in (select o.shape
                              from   objects as o -- Potential merge candidates
                              where  o.shape <> ?1
                              and    exists (select 1
                                             from   objects as _o -- Selected shape
                                             where  _o.shape = ?1
                                             and    "connects?"(_o.connectors, _o.kind, _o.x, _o.y, o.connectors, o.kind, o.x, o.y)))
          "#, params![shape])? > 0 {
            // If shape is completed and has any doors, open them.
            if tx.query_row(r#"
              select coalesce(bool_and("is complete?"(o.connectors,o.kind,o.x,o.y)) and bool_or(o.kind = 'Door'), false)
              from   objects as o
              where  o.shape = ?1
            "#, params![shape], |row| row.get(0))? {
              // Keep the completed shape positions
              tx.execute("create temporary table completed_shape as select * from objects as o where o.shape = ?1", params![shape])?;

              // Open all doors
              tx.execute(r#"
                delete from objects
                  where shape = ?1
                  and   kind = 'Door'
                  and   (connectors & 240) > 0
                  and   (connectors &  15) = 0
              "#, params![shape])?;
              // Remove partial doors from objects
              tx.execute(r#"
                update objects
                  set   connectors = connectors & 15,
                        kind = 'None'
                  where shape = ?1
                  and   kind = 'Door'
              "#, params![shape])?;
              // Update shapes
              while {
                let new_shape: i32 = tx.query_row("select nextval('shape_seq_id')", params![], |row| row.get(0))?;
                tx.execute(r#"
                update objects
                set    shape = ?2
                where  (x,y) in (
                  with recursive one_shape(shape,connectors,kind,x,y) as (
                    (select o.shape, o.connectors, o.kind, o.x, o.y
                     from   objects as o
                     where  shape = ?1
                     limit  1)
                      union
                    select o.shape, o.connectors, o.kind, o.x, o.y
                    from   one_shape as os, objects as o
                    where  os.shape = o.shape
                    and    "connects?"(os.connectors, os.kind, os.x, os.y, o.connectors, o.kind, o.x, o.y)
                  )
                  select (os.x,os.y)
                  from   one_shape as os
                )
              "#, params![shape, new_shape])? > 0 } { }
              // Query all objects that have previously been `shape` and set
              let mut selected_shape = Some(shape);
              let there_shape =
                State::query_objects_via_statement(
                  tx.prepare(r#"
                    select cs.id, o.shape, o.connectors, o.kind::text, cs.x, cs.y
                    from   completed_shape as cs left join objects as o on (cs.x,cs.y) = (o.x,o.y);
                  "#)?, params![],
                  |row| {
                    let id = row.get(0)?;
                    let o_shape: Option<i32> = row.get(1)?;
                    let pos = (row.get(4)?,row.get(5)?);
                    if pos == there { selected_shape = o_shape }
                    if let Some(shape) = o_shape {
                      Ok(Object::new_with_color(id, shape, row.get(2)?, row.get(3)?, pos, Some(Color::DarkGrey)))
                    } else {
                      Ok(Object::new(id, 0, 0, "Removed".to_string(), pos))
                    }
                  }
                )?;
                // Clean up
                tx.execute("drop table completed_shape", params![])?;
              return Ok(Some((here_shape,there_shape,selected_shape)));
            }
          }
          return Ok(Some((here_shape, State::objects_by_shape_via_tx_with_color(tx, shape, Some(Color::White))?, Some(shape))));
        }
      }
    }
    Ok(None)
  }

  /// Return turn counter and true, if the level is complete
  fn turn_state(&self) -> Result<(i32,bool), duckdb::Error> {
    self.db.query_row(r#"
      select coalesce((select max(u.turn)+1 from undo as u), (select min(r.turn)-1 from redo as r), 0) as turn,
             (select bool_and("is complete?"(o.connectors,o.kind,o.x,o.y)) as is_complete
              from   objects as o
              where  o.connectors > 0)
    "#, params![], |row| Ok((row.get(0)?,row.get(1)?)))
  }

  /// Print the turn counter and an indicator, if the level is complete
  fn print_turn_counter(&self) -> error::IOResult {
    let (turn_count,is_complete) = self.turn_state()?;
    self.state_control_send.send(
      StateControlPayload::TurnCounter(
        self.db.query_row("select max(o.y) from objects as o", params![], |row| row.get(0)).optional()?.map(|v_pos: u16| v_pos+1).unwrap_or(0),
        turn_count,
        is_complete
      ))?;
    Ok(())
  }

  /// Move cursor in a `direction` and notify the updated position to the controller, if the cursor moved to a new position
  /// If the cursor has an object id selected, move the object with the object id as well, then notify the controller
  fn move_cursor(&mut self, direction: Direction) -> error::IOResult {
    let cursor_here = self.cursor_position();
    if let Some(cursor_there) = State::move_cursor_to(&cursor_here, direction, self.board_size, self.selected_shape.is_some()) {
      let mut do_cursor_move = cursor_here != cursor_there;
      let mut do_shape_move  = false;
      let mut selected_shape = self.selected_shape;
      let mut db             = self.db.try_clone()?;
      let tx                 = db.transaction()?;
      if do_cursor_move {
        if let Some(shape) = selected_shape {
          match State::move_shape(&tx, shape, cursor_here, cursor_there, self.board_size) {
            Ok(None)                                                => { do_cursor_move = false },
            Ok(Some((here_shape, there_shape, new_selected_shape))) => {
              do_shape_move  = here_shape.len() != there_shape.len() || here_shape.iter().enumerate().any(|(i,here)| here.pos != there_shape[i].pos);
              do_cursor_move = do_shape_move;
              selected_shape = new_selected_shape;
              if do_shape_move {
                self.state_control_send.send(StateControlPayload::MoveShape(here_shape,there_shape))?; // Note: An object always moves before the cursor.
              }
            },
            Err(e) => {
              log::error!("State: Tried moving a selected shape ({}), but it failed: {}", shape, e);
              return Err(e)
            },
          }
        }
        if do_cursor_move {
          self.cursor_pos = cursor_there;
          self.state_control_send.send(StateControlPayload::SetCursorPosition((self.cursor_pos.0, self.cursor_pos.1)))?;
          if self.selected_shape != selected_shape {
            self.selected_shape = selected_shape;
            if let Some(shape) = self.selected_shape {
              self.state_control_send.send(StateControlPayload::PrintObjects(self.objects_by_shape_with_color(shape, Some(Color::White))?))?;
            }
          }
        }
      }
      tx.commit()?;
      if do_shape_move { self.print_turn_counter()?; }
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

        insert or replace into objects(id,shape,connectors,kind,x,y)
          select columns(* exclude (turn)) from undo as u where u.turn = (select max(u.turn) from undo u);

        delete from undo
          where turn = (select max(u.turn) from undo u);
      "#)?;
      tx.commit()?;
      self.selected_shape = None;
      self.clear_print_all()?;
      self.print_turn_counter()?;
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

        insert or replace into objects(id,shape,connectors,kind,x,y)
          select columns(* exclude (turn)) from redo as r where r.turn = (select min(r.turn) from redo r);

        delete from redo
          where turn = (select min(r.turn) from redo r);
      "#)?;
      tx.commit()?;
      self.selected_shape = None;
      self.clear_print_all()?;
      self.print_turn_counter()?;
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
    let save_file_path_string = self.get_save_file_path();
    let save_file_path = Path::new(&save_file_path_string);
    if Path::exists(save_file_path) {
      if let Err(e) = zip_extract::extract(fs::File::open(Path::new(save_file_path))?, Path::new("."), false) {
        log::error!("Load file at path {} failed: {}", &save_file_path_string, e);
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
    }
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
    fn add_object(state: &State, shape: i32, conectors: i32, kind: String, x: u16, y: u16) -> duckdb::Result<usize> { state.db.execute(r#"insert into objects(shape,connectors,kind,x,y) select ?1,?2,?3,?4,?5"#, params![shape,conectors,kind,x,y]) }

    // Wrapper function to query an object by shape via transaction
    fn object_by_id(state: &State, shape: i32) -> duckdb::Result<Option<Object>> {
      state.db.query_row(r#"
        select o.id, o.shape, o.connectors, o.kind::text, o.x, o.y
        from   objects as o
        where  o.shape = ?1
      "#, params![shape], |row| Ok(Object::new(row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, (row.get(4)?, row.get(5)?)))).optional() }

    #[test]
    // Populate the database with one object {id: 1, shape: 1, pos: (2,3)} and test object_by_id and object_by_pos
    fn object_by_id_and_pos_database() -> error::IOResult {
      const SHAPE: i32      = 1;
      const CONNECTORS: i32 = 15;
      const X: u16          = 2;
      const Y: u16          = 3;
      let (state, _, _) = State::new()?;
      state.init_database()?;
      assert_eq!(add_object(&state, SHAPE, CONNECTORS, "None".to_string(), X, Y)?, 1);
      assert_eq!(object_by_id(&state,1)?, Some(Object::new(1, SHAPE, CONNECTORS, "None".to_string(), (X,Y))));
      assert_eq!(state.object_by_pos((X,Y))?, Some(Object::new(1, SHAPE, CONNECTORS, "None".to_string(), (X,Y))));
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
          match dummy_recv.recv() {
            Ok(StateControlPayload::SetCursorPosition(actual_pos)) => assert_eq!(actual_pos, expected_pos),
            Ok(payload)                                            => panic!("Did not receive SetCursorPosition: {:?}", payload),
            Err(e)                                                 => panic!("Failed to receive SetCursorPosition: {}", e)
          }
        }
        fn assert_received_shape_movement(dummy_recv: &Receiver<StateControlPayload>, expected_here: Object, expected_there: Object) {
          match dummy_recv.recv() {
            Ok(StateControlPayload::MoveShape(here_shape,there_shape)) =>  {
              assert_eq!(here_shape.len(), 1);
              assert_eq!(there_shape.len(), 1);
              assert_eq!((here_shape[0].clone(), there_shape[0].clone()), (expected_here, expected_there))
            },
            Ok(payload)                                            => panic!("Did not receive MoveShape: {:?}", payload),
            Err(e)                                                 => panic!("Failed to receive MoveShape: {}", e)
          }
        }
        fn assert_received_print_objects(dummy_recv: &Receiver<StateControlPayload>, expected_obj: Object) {
          match dummy_recv.recv() {
            Ok(StateControlPayload::PrintObjects(shape)) =>  {
              assert_eq!(shape.len(), 1);
              assert_eq!(shape[0], expected_obj)
            },
            Ok(payload)                                            => panic!("Did not receive PrintObjects: {:?}", payload),
            Err(e)                                                 => panic!("Failed to receive PrintObjects: {}", e)
          }
        }
        fn assert_received_turn_counter(dummy_recv: &Receiver<StateControlPayload>, expected_y_pos: u16, expected_turn: i32, expected_complete: bool) {
          match dummy_recv.recv() {
            Ok(StateControlPayload::TurnCounter(y_pos,turn,complete)) =>  {
              assert_eq!((y_pos,turn,complete),(expected_y_pos,expected_turn,expected_complete));
            },
            Ok(payload)                                            => panic!("Did not receive TurnCounter: {:?}", payload),
            Err(e)                                                 => panic!("Failed to receive TurnCounter: {}", e)
          }
        }
        assert_received_cursor_position(&dummy_recv, (INITIAL_CURSOR_POS_X+1,INITIAL_CURSOR_POS_Y));
        assert_received_cursor_position(&dummy_recv, (INITIAL_CURSOR_POS_X+2,INITIAL_CURSOR_POS_Y));
        assert_received_cursor_position(&dummy_recv, (INITIAL_CURSOR_POS_X+2,INITIAL_CURSOR_POS_Y+1));
        assert_received_cursor_position(&dummy_recv, (INITIAL_CURSOR_POS_X+2,INITIAL_CURSOR_POS_Y+2));
        assert_received_cursor_position(&dummy_recv, (INITIAL_CURSOR_POS_X+2,INITIAL_CURSOR_POS_Y+3));
          assert_received_print_objects(&dummy_recv, Object::new_with_color(1, SHAPE, CONNECTORS, "None".to_string(), (X,Y), Some(Color::White)));
         assert_received_shape_movement(&dummy_recv, Object::new_with_color(1, SHAPE, CONNECTORS, "None".to_string(), (X,Y), None), Object::new_with_color(1, SHAPE, CONNECTORS, "None".to_string(), (X,Y+1), Some(Color::White)));
        assert_received_cursor_position(&dummy_recv, (INITIAL_CURSOR_POS_X+2,INITIAL_CURSOR_POS_Y+4));
           assert_received_turn_counter(&dummy_recv, 5, 1, false);
         assert_received_shape_movement(&dummy_recv, Object::new(1, SHAPE, CONNECTORS, "None".to_string(), (X,Y+1)), Object::new_with_color(1, SHAPE, CONNECTORS, "None".to_string(), (X-1,Y+1), Some(Color::White)));
        assert_received_cursor_position(&dummy_recv, (INITIAL_CURSOR_POS_X+1,INITIAL_CURSOR_POS_Y+4));
           assert_received_turn_counter(&dummy_recv, 5, 2, false);
         assert_received_shape_movement(&dummy_recv, Object::new(1, SHAPE, CONNECTORS, "None".to_string(), (X-1,Y+1)), Object::new_with_color(1, SHAPE, CONNECTORS, "None".to_string(), (X,Y+1), Some(Color::White)));
        assert_received_cursor_position(&dummy_recv, (INITIAL_CURSOR_POS_X+2,INITIAL_CURSOR_POS_Y+4));
           assert_received_turn_counter(&dummy_recv, 5, 3, false);
         assert_received_shape_movement(&dummy_recv, Object::new(1, SHAPE, CONNECTORS, "None".to_string(), (X,Y+1)), Object::new_with_color(1, SHAPE, CONNECTORS, "None".to_string(), (X,Y), Some(Color::White)));
        assert_received_cursor_position(&dummy_recv, (INITIAL_CURSOR_POS_X+2,INITIAL_CURSOR_POS_Y+3));
           assert_received_turn_counter(&dummy_recv, 4, 4, false);
         assert_received_shape_movement(&dummy_recv, Object::new(1, SHAPE, CONNECTORS, "None".to_string(), (X,Y)), Object::new_with_color(1, SHAPE, CONNECTORS, "None".to_string(), (X,Y+1), Some(Color::White)));
        assert_received_cursor_position(&dummy_recv, (INITIAL_CURSOR_POS_X+2,INITIAL_CURSOR_POS_Y+4));
           assert_received_turn_counter(&dummy_recv, 5, 5, false);
          assert_received_print_objects(&dummy_recv, Object::new_with_color(1, SHAPE, CONNECTORS, "None".to_string(), (X,Y+1), Some(Color::DarkGrey)));
        assert_received_cursor_position(&dummy_recv, (INITIAL_CURSOR_POS_X+3,INITIAL_CURSOR_POS_Y+4));
      });

      state.init_database()?;
      assert_eq!(add_object(&state, SHAPE, CONNECTORS, "None".to_string(), X, Y)?, 1);
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
      assert_eq!(object_by_id(&state,1)?, Some(Object::new(1, SHAPE, CONNECTORS, "None".to_string(), (X,Y))));
      assert_eq!(state.selected_shape, Some(1));
      state.move_cursor(Direction::Down)?;
      assert_eq!(state.cursor_position(), (INITIAL_CURSOR_POS_X+2, INITIAL_CURSOR_POS_Y+4));
      assert_eq!(object_by_id(&state,1)?, Some(Object::new(1, SHAPE, CONNECTORS, "None".to_string(), (X,Y+1))));
      assert_eq!(state.selected_shape, Some(1));
      state.move_cursor(Direction::Left)?;
      assert_eq!(state.cursor_position(), (INITIAL_CURSOR_POS_X+1, INITIAL_CURSOR_POS_Y+4));
      assert_eq!(object_by_id(&state,1)?, Some(Object::new(1, SHAPE, CONNECTORS, "None".to_string(), (X-1,Y+1))));
      assert_eq!(state.selected_shape, Some(1));
      state.move_cursor(Direction::Right)?;
      assert_eq!(state.cursor_position(), (INITIAL_CURSOR_POS_X+2, INITIAL_CURSOR_POS_Y+4));
      assert_eq!(object_by_id(&state,1)?, Some(Object::new(1, SHAPE, CONNECTORS, "None".to_string(), (X,Y+1))));
      assert_eq!(state.selected_shape, Some(1));
      state.move_cursor(Direction::Up)?;
      assert_eq!(state.cursor_position(), (INITIAL_CURSOR_POS_X+2, INITIAL_CURSOR_POS_Y+3));
      assert_eq!(object_by_id(&state,1)?, Some(Object::new(1, SHAPE, CONNECTORS, "None".to_string(), (X,Y))));
      assert_eq!(state.selected_shape, Some(1));
      state.move_cursor(Direction::Down)?;
      assert_eq!(state.cursor_position(), (INITIAL_CURSOR_POS_X+2, INITIAL_CURSOR_POS_Y+4));
      assert_eq!(object_by_id(&state,1)?, Some(Object::new(1, SHAPE, CONNECTORS, "None".to_string(), (X,Y+1))));
      assert_eq!(state.selected_shape, Some(1));
      state.toggle_select_shape()?;
      assert_eq!(state.cursor_position(), (INITIAL_CURSOR_POS_X+2, INITIAL_CURSOR_POS_Y+4));
      assert_eq!(object_by_id(&state,1)?, Some(Object::new(1, SHAPE, CONNECTORS, "None".to_string(), (X,Y+1))));
      assert_eq!(state.selected_shape, None);
      state.move_cursor(Direction::Right)?;
      assert_eq!(state.cursor_position(), (INITIAL_CURSOR_POS_X+3, INITIAL_CURSOR_POS_Y+4));
      assert_eq!(object_by_id(&state,1)?, Some(Object::new(1, SHAPE, CONNECTORS, "None".to_string(), (X,Y+1))));
      assert_eq!(state.selected_shape, None);
      if !dummy_thread.is_finished() { thread::sleep(time::Duration::from_secs(WAIT_FOR_DUMMY_THREAD_IN_SECS)); if !dummy_thread.is_finished() { panic!("Thread did not finish after waiting for it for at least {} seconds", WAIT_FOR_DUMMY_THREAD_IN_SECS) } }
      Ok(())
    }

    #[test]
    /// ┌┐
    /// └┘
    fn simple_complete() -> error::IOResult {
      let (state, _, _) = State::new()?;
      state.init_database()?;
      add_object(&state, 1, 0b0110, "None".to_string(), 1, 1)?; // ┌
      add_object(&state, 1, 0b0011, "None".to_string(), 2, 1)?; // ┐
      add_object(&state, 1, 0b1001, "None".to_string(), 2, 2)?; // ┘
      add_object(&state, 1, 0b1100, "None".to_string(), 1, 2)?; // └
      assert!(state.turn_state()?.1);
      Ok(())
    }

    #[test]
    /// ┐┌
    /// └┘
    fn simple_single_shape_incomplete() -> error::IOResult {
      let (state, _, _) = State::new()?;
      state.init_database()?;
      add_object(&state, 1, 0b0110, "None".to_string(), 2, 1)?; // ┌
      add_object(&state, 1, 0b0011, "None".to_string(), 1, 1)?; // ┐
      add_object(&state, 1, 0b1001, "None".to_string(), 2, 2)?; // ┘
      add_object(&state, 1, 0b1100, "None".to_string(), 1, 2)?; // └
      assert!(!state.turn_state()?.1);
      Ok(())
    }

    #[test]
    /// ┐ ┌
    /// └ ┘
    fn simple_two_shape_incomplete() -> error::IOResult {
      let (state, _, _) = State::new()?;
      state.init_database()?;
      add_object(&state, 2, 0b0110, "None".to_string(), 3, 1)?; // ┌
      add_object(&state, 1, 0b0011, "None".to_string(), 1, 1)?; // ┐
      add_object(&state, 2, 0b1001, "None".to_string(), 3, 2)?; // ┘
      add_object(&state, 1, 0b1100, "None".to_string(), 1, 2)?; // └
      assert!(!state.turn_state()?.1);
      Ok(())
    }

    #[test]
    /// ┌┐ ┌┐
    /// └┘ └┘
    fn simple_two_shape_complete() -> error::IOResult {
      let (state, _, _) = State::new()?;
      state.init_database()?;
      add_object(&state, 1, 0b0110, "None".to_string(), 1, 1)?; // ┌
      add_object(&state, 1, 0b0011, "None".to_string(), 2, 1)?; // ┐
      add_object(&state, 1, 0b1001, "None".to_string(), 2, 2)?; // ┘
      add_object(&state, 1, 0b1100, "None".to_string(), 1, 2)?; // └
      add_object(&state, 2, 0b0110, "None".to_string(), 4, 1)?; // ┌
      add_object(&state, 2, 0b0011, "None".to_string(), 5, 1)?; // ┐
      add_object(&state, 2, 0b1001, "None".to_string(), 5, 2)?; // ┘
      add_object(&state, 2, 0b1100, "None".to_string(), 4, 2)?; // └
      assert!(state.turn_state()?.1);
      Ok(())
    }

    #[test]
    /// ┌┐═┌┐    ┌┐ ┌┐    ┌┐ ┌┐
    /// │╞ ╡│ -> │╞═╡│ -> ││ ││
    /// └┘ └┘    └┘ └┘    └┘ └┘
    fn shapes_with_doors() -> error::IOResult {
      let (state, _, _) = State::new()?;
      state.init_database()?;
      let mut shape: i32 = state.db.query_row("select nextval('shape_seq_id')", params![], |row| row.get(0))?;
      add_object(&state, shape, 0b00000110, "None".to_string(), 1, 1)?; // ┌
      add_object(&state, shape, 0b00000011, "None".to_string(), 2, 1)?; // ┐
      add_object(&state, shape, 0b00001001, "None".to_string(), 2, 3)?; // ┘
      add_object(&state, shape, 0b00001100, "None".to_string(), 1, 3)?; // └
      add_object(&state, shape, 0b00001010, "None".to_string(), 1, 2)?; // |
      add_object(&state, shape, 0b01001010, "Door".to_string(), 2, 2)?; // ╞

      shape = state.db.query_row("select nextval('shape_seq_id')", params![], |row| row.get(0))?;
      add_object(&state, shape, 0b01010000, "Door".to_string(), 3, 1)?; // ═

      shape = state.db.query_row("select nextval('shape_seq_id')", params![], |row| row.get(0))?;
      add_object(&state, shape, 0b00000110, "None".to_string(), 4, 1)?; // ┌
      add_object(&state, shape, 0b00000011, "None".to_string(), 5, 1)?; // ┐
      add_object(&state, shape, 0b00001001, "None".to_string(), 5, 3)?; // ┘
      add_object(&state, shape, 0b00001100, "None".to_string(), 4, 3)?; // └
      add_object(&state, shape, 0b00001010, "None".to_string(), 5, 2)?; // |
      add_object(&state, shape, 0b00011010, "Door".to_string(), 4, 2)?; // ╡

      let mut db = state.db.try_clone()?;
      let tx     = db.transaction()?;
      let (here,there,selected_object) = State::move_shape(&tx, 2, (3,1), (3,2), (10,10))?.expect("Shape returned `None`, where `Some` was expeced");
      assert_eq!(here.len(), 1);
      assert_eq!(there.len(), 13);
      assert_eq!(selected_object, None);
      tx.commit()?;

      assert_eq!(state.object_by_pos((3,2))?, None); // Removed
      assert_eq!(state.object_by_pos((2,2))?, Some(Object::new( 6, shape+1, 0b00001010, "None".to_string(), (2,2)))); // ╞ → |
      assert_eq!(state.object_by_pos((4,2))?, Some(Object::new(13, shape+2, 0b00001010, "None".to_string(), (4,2)))); // ╡ → |
      assert_eq!(state.db.query_row("select count(distinct o.shape) from objects as o", params![], |row| row.get(0)), Ok(2));
      assert!(state.turn_state()?.1);

      Ok(())
    }

    #[test]
    /// ┌┐       ┌┐ ┌┐    ┌┐ ┌┐
    /// │╞═┌┐ -> │╞═╡│ -> ││ ││
    /// └┘ ╡│    └┘ └┘    └┘ └┘
    ///    └┘
    fn shapes_with_doors_keep_selection() -> error::IOResult {
      let (state, _, _) = State::new()?;
      state.init_database()?;
      let mut shape: i32 = state.db.query_row("select nextval('shape_seq_id')", params![], |row| row.get(0))?;
      add_object(&state, shape, 0b00000110, "None".to_string(), 1, 1)?; // ┌
      add_object(&state, shape, 0b00000011, "None".to_string(), 2, 1)?; // ┐
      add_object(&state, shape, 0b00001001, "None".to_string(), 2, 3)?; // ┘
      add_object(&state, shape, 0b00001100, "None".to_string(), 1, 3)?; // └
      add_object(&state, shape, 0b00001010, "None".to_string(), 1, 2)?; // |
      add_object(&state, shape, 0b01001010, "Door".to_string(), 2, 2)?; // ╞
      add_object(&state, shape, 0b01010000, "Door".to_string(), 3, 2)?; // ═

      shape = state.db.query_row("select nextval('shape_seq_id')", params![], |row| row.get(0))?;
      add_object(&state, shape, 0b00000110, "None".to_string(), 4, 2)?; // ┌
      add_object(&state, shape, 0b00000011, "None".to_string(), 5, 2)?; // ┐
      add_object(&state, shape, 0b00001001, "None".to_string(), 5, 4)?; // ┘
      add_object(&state, shape, 0b00001100, "None".to_string(), 4, 4)?; // └
      add_object(&state, shape, 0b00001010, "None".to_string(), 5, 3)?; // |
      add_object(&state, shape, 0b00011010, "Door".to_string(), 4, 3)?; // ╡

      let mut db = state.db.try_clone()?;
      let tx     = db.transaction()?;
      let (here,there,selected_object) = State::move_shape(&tx, 2, (4,2), (4,1), (10,10))?.expect("Shape returned `None`, where `Some` was expeced");
      assert_eq!(here.len(), 6);
      assert_eq!(there.len(), 13);
      assert_eq!(selected_object, Some(4));
      tx.commit()?;

      assert_eq!(state.object_by_pos((3,2))?, None); // Removed
      assert_eq!(state.object_by_pos((2,2))?, Some(Object::new( 6, shape+1, 0b00001010, "None".to_string(), (2,2)))); // ╞ → |
      assert_eq!(state.object_by_pos((4,2))?, Some(Object::new(13, shape+2, 0b00001010, "None".to_string(), (4,2)))); // ╡ → |
      assert_eq!(state.db.query_row("select count(distinct o.shape) from objects as o", params![], |row| row.get(0)), Ok(2));
      assert!(state.turn_state()?.1);

      Ok(())
    }
}