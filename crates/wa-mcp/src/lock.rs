//! Verrou exclusif par compte (`accounts/<alias>/lock`).
//!
//! Deux clients sur la même session se déconnectent mutuellement
//! (StreamReplaced) et l'un abandonne pour de bon : une seule instance de wa-mcp
//! par compte. Le verrou tombe avec le process, même tué.

use std::fs::{File, OpenOptions, TryLockError};
use std::path::Path;

use crate::domain::AccountAlias;

#[derive(Debug, thiserror::Error)]
pub enum LockError {
    #[error(
        "le compte {0} est déjà utilisé par une autre instance de wa-mcp (serveur MCP d'un hôte, `run` dans un terminal...) : l'arrêter d'abord"
    )]
    Busy(AccountAlias),
    #[error("verrou du compte : {0}")]
    Io(#[from] std::io::Error),
}

/// Prend le verrou ; il est tenu tant que le `File` rendu vit.
pub fn lock(data_dir: &Path, account: &AccountAlias) -> Result<File, LockError> {
    let dir = data_dir.join("accounts").join(account.as_str());
    std::fs::create_dir_all(&dir)?;
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(dir.join("lock"))?;
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(TryLockError::WouldBlock) => Err(LockError::Busy(account.clone())),
        Err(TryLockError::Error(e)) => Err(e.into()),
    }
}
