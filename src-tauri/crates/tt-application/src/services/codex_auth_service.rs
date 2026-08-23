use std::path::PathBuf;
use std::sync::Arc;

use crate::dto::codex_auth_dto::{CodexAuthImportResultDto, CodexAuthImportTargetDto};
use crate::errors::ApplicationError;
use tt_ports::repositories::codex_auth_repository::CodexAuthRepository;

pub struct CodexAuthService {
    repository: Arc<dyn CodexAuthRepository>,
}

impl CodexAuthService {
    pub fn new(repository: Arc<dyn CodexAuthRepository>) -> Self {
        Self { repository }
    }

    pub async fn prepare_import_staging_path(
        &self,
    ) -> Result<CodexAuthImportTargetDto, ApplicationError> {
        let path = self.repository.prepare_import_staging_path().await?;
        Ok(CodexAuthImportTargetDto {
            file_path: path.to_string_lossy().into_owned(),
        })
    }

    pub async fn import_auth_file(
        &self,
        file_path: &str,
        expected_existing: Option<&str>,
    ) -> Result<CodexAuthImportResultDto, ApplicationError> {
        let path = require_path(file_path)?;
        let expected_existing = expected_existing
            .map(str::trim)
            .filter(|value| !value.is_empty());
        self.repository
            .import_auth_file(&path, expected_existing)
            .await
            .map(Into::into)
            .map_err(Into::into)
    }

    pub async fn discard_import_staging_path(
        &self,
        file_path: &str,
    ) -> Result<(), ApplicationError> {
        let path = require_path(file_path)?;
        self.repository
            .discard_import_staging_path(&path)
            .await
            .map_err(Into::into)
    }
}

fn require_path(raw: &str) -> Result<PathBuf, ApplicationError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(ApplicationError::ValidationError(
            "Codex auth import path is required".to_string(),
        ));
    }

    Ok(PathBuf::from(trimmed))
}
