//! Small extension traits that pair naturally with anyhow + tracing for
//! error observability without forcing callers to write log-then-return
//! boilerplate.

use std::panic::Location;

use crate::prelude::*;

/// Attach a call-site log line to an error without consuming the `Result`.
pub trait LogErrExt: Sized {
    fn log(self) -> Self;
}

impl<T, E: std::fmt::Debug> LogErrExt for std::result::Result<T, E> {
    #[track_caller]
    fn log(self) -> Self {
        let caller = Location::caller();
        self.inspect_err(|err| error!("From {}:{}: {err:?}", caller.file(), caller.line()))
    }
}

/// Convert `None` to an `Err` and log the call site in one call.
pub trait LogNoneExt<T>: Sized {
    fn log(self) -> std::result::Result<T, Error>;
}

impl<T> LogNoneExt<T> for Option<T> {
    #[track_caller]
    fn log(self) -> std::result::Result<T, Error> {
        self.ok_or_else(|| anyhow!("Value was None.")).log()
    }
}
