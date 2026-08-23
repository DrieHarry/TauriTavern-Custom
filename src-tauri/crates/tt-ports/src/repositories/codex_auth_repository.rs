use std::path::{Path, PathBuf};

use async_trait::async_trait;
use tt_domain::errors::DomainError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodexAuthImportOutcome {
    Imported { replaced_existing: bool },
    AlreadyCurrent,
    RequiresConfirmation { confirmation: String },
}

#[async_trait]
pub trait CodexAuthRepository: Send + Sync {
    async fn prepare_import_staging_path(&self) -> Result<PathBuf, DomainError>;

    async fn import_auth_file(
        &self,
        source_path: &Path,
        expected_existing: Option<&str>,
    ) -> Result<CodexAuthImportOutcome, DomainError>;

    async fn discard_import_staging_path(&self, path: &Path) -> Result<(), DomainError>;
}
