#[macro_use]
extern crate crossterm;
extern crate chrono;

mod conductor;
mod common;

fn main() { if let Err(e) = conductor::run() {println!("{:?}", e); } }
