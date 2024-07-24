use std::panic::Location;

use crate::prelude::*;

pub trait LogErrExt<E: std::fmt::Debug>: Sized {
    fn inspect_err<F: FnOnce(&E)>(self, f: F) -> Self;

    #[track_caller]
    fn log(self) -> Self {
        let caller = Location::caller();
        self.inspect_err(|err| error!("From {}:{}: {err:?}", caller.file(), caller.line()))
    }
}

impl<T, E> LogErrExt<E> for std::result::Result<T, E>
where
    E: std::fmt::Debug,
{
    fn inspect_err<F: FnOnce(&E)>(self, f: F) -> Self {
        self.inspect_err(f)
    }
}

pub trait LogNoneExt<T>: Sized {
    fn ok_or<E>(self, err: E) -> std::result::Result<T, E>;

    #[track_caller]
    fn log(self) -> std::result::Result<T, Error> {
        self.ok_or(anyhow!("Value was None.")).log()
    }
}

impl<T> LogNoneExt<T> for Option<T> {
    fn ok_or<E>(self, err: E) -> std::result::Result<T, E> {
        self.ok_or(err)
    }
}

// pub trait FallibleIteratorExt<T, E, I>
// where I: Iterator<Item = std::result::Result<T, E>> {
//     fn fallible(self) -> fallible_iterator::Convert<I>;
// }

// impl<T, E, I> FallibleIteratorExt<T, E, I> for I
// where I: Iterator<Item = std::result::Result<T, E>> {
//     fn fallible(self) -> fallible_iterator::Convert<I> {
//         fallible_iterator::convert(self)
//     }
// }

pub trait FallibleIteratorExt<T, E, I>
where
    I: Iterator<Item = T> + Sized,
{
    fn fallible(
        self,
    ) -> fallible_iterator::Convert<impl Iterator<Item = std::result::Result<T, E>>>;
}

impl<T, I> FallibleIteratorExt<T, Error, I> for I
where
    I: Iterator<Item = T> + Sized,
{
    fn fallible(
        self,
    ) -> fallible_iterator::Convert<impl Iterator<Item = std::result::Result<T, Error>>> {
        fallible_iterator::convert(self.map(Ok::<T, Error>))
    }
}
