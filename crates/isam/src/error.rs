/// All errors that can be produced by this library.
///
/// `thiserror::Error` generates the boilerplate `std::error::Error` impl,
/// and the `#[error("...")]` attribute defines the human-readable message
/// for each variant.
use thiserror::Error;

#[derive(Debug, Error)]
pub enum IsamError {
    /// Wraps any std::io::Error (e.g. file not found, permission denied).
    /// The `#[from]` attribute lets the `?` operator convert io::Error
    /// automatically — no explicit `.map_err(...)` needed.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Wraps bincode serialization/deserialization errors.
    #[error("serialization error: {0}")]
    Bincode(#[from] Box<bincode::ErrorKind>),

    /// The requested key does not exist (used for update on missing key).
    #[error("key not found")]
    KeyNotFound,

    /// Attempted to insert a key that already exists in the index.
    #[error("duplicate key")]
    DuplicateKey,

    /// The index file is corrupt or was created by an incompatible version.
    #[error("index file is corrupt: {0}")]
    CorruptIndex(String),

    /// A thread panicked while holding the database lock.
    #[error("mutex poisoned: a thread panicked while holding the database lock")]
    LockPoisoned,
}

/// Convenience alias — every fallible function in this crate returns this.
pub type IsamResult<T> = Result<T, IsamError>;
