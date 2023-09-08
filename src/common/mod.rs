use std::{time, thread};

pub fn wait_minus_elapsed(duration: time::Duration, elapsed: time::Duration) {
    if let Some(wait_duration) = duration.checked_sub(elapsed) {
        thread::sleep(wait_duration)
    };
}