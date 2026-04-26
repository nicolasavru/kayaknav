//! Common re-exports for noaa_tides internals. Use via `use crate::prelude::*;`
//! to get `anyhow` error types and tracing macros in a single import.

pub use anyhow::Context;
pub use anyhow::Error;
pub use anyhow::Result;
pub use anyhow::anyhow;
pub use anyhow::bail;
pub use tracing::Span;
pub use tracing::debug;
pub use tracing::error;
pub use tracing::info;
pub use tracing::instrument;
pub use tracing::warn;

pub use crate::error_utils::*;
