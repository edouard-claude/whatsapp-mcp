//! Verrou exclusif par compte (`accounts/<alias>/lock`).
//!
//! Deux clients sur la même session se déconnectent mutuellement
//! (StreamReplaced) et l'un abandonne pour de bon : une seule instance de wa-mcp
//! par compte. Le verrou tombe avec le process, même tué.

use std::fs::{File, OpenOptions, TryLockError};
use std::path::Path;
use std::sync::Arc;

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

/// Verrous pris en cours de route (appairage depuis MCP), tenus jusqu'à la fin
/// du process. Partagés entre toutes les sessions MCP d'une instance : la
/// session WhatsApp ouverte dans le bridge survit à la session MCP qui l'a
/// demandée, son verrou doit donc lui survivre aussi.
#[derive(Clone, Default)]
pub struct Held(Arc<parking_lot::Mutex<Vec<File>>>);

impl Held {
    pub fn keep(&self, lock: File) {
        self.0.lock().push(lock);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Un compte appairé depuis une session MCP reste verrouillé quand cette
    /// session se ferme : c'est l'instance qui tient son verrou.
    #[test]
    fn verrou_d_un_appairage_survit_a_la_session() {
        let dir = std::env::temp_dir().join(format!("wa-held-{}", std::process::id()));
        let account: AccountAlias = "pro".parse().unwrap_or_else(|e| panic!("alias : {e}"));
        let instance = Held::default();
        {
            // Ce que fait `session_pair` dans le `WaServer` d'une session.
            let session = instance.clone();
            let lock = lock(&dir, &account).unwrap_or_else(|e| panic!("verrou : {e}"));
            session.keep(lock);
        } // la session se ferme : son `WaServer` et sa poignée disparaissent
        assert!(matches!(lock(&dir, &account), Err(LockError::Busy(_))));
        drop(instance); // fin de l'instance
        assert!(lock(&dir, &account).is_ok());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
