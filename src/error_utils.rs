use std::panic::Location;

use crate::prelude::*;

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

pub trait LogNoneExt<T>: Sized {
    fn log(self) -> std::result::Result<T, Error>;
}

impl<T> LogNoneExt<T> for Option<T> {
    #[track_caller]
    fn log(self) -> std::result::Result<T, Error> {
        self.ok_or_else(|| anyhow!("Value was None.")).log()
    }
}
