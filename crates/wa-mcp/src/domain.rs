//! Types du domaine.

use std::fmt;
use std::str::FromStr;

/// Alias d'un compte WhatsApp : `[a-z0-9][a-z0-9_-]{0,31}`.
///
/// Sert de nom de répertoire (`accounts/<alias>/`) : la validation empêche toute
/// traversée de chemin. Le bridge applique la même règle.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct AccountAlias(Box<str>);

#[derive(Debug, thiserror::Error)]
#[error("alias de compte invalide {0:?} : attendu [a-z0-9][a-z0-9_-]{{0,31}}")]
pub struct InvalidAlias(String);

impl AccountAlias {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for AccountAlias {
    type Err = InvalidAlias;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut chars = s.chars();
        let first_ok = chars
            .next()
            .is_some_and(|c| c.is_ascii_lowercase() || c.is_ascii_digit());
        let rest_ok =
            chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-');
        if first_ok && rest_ok && s.len() <= 32 {
            Ok(AccountAlias(s.into()))
        } else {
            Err(InvalidAlias(s.to_owned()))
        }
    }
}

impl fmt::Display for AccountAlias {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alias_rules() {
        for ok in ["perso", "pro-2", "a", "0_x"] {
            assert!(ok.parse::<AccountAlias>().is_ok(), "{ok}");
        }
        for ko in ["", "Perso", "-a", "../x", "a/b", "é", &"a".repeat(33)] {
            assert!(ko.parse::<AccountAlias>().is_err(), "{ko}");
        }
    }
}
