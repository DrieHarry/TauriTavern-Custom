use serde::Serialize;

use tt_ports::repositories::codex_auth_repository::CodexAuthImportOutcome;

pub const CODEX_AUTH_IMPORT_STATUS_IMPORTED: &str = "imported";
pub const CODEX_AUTH_IMPORT_STATUS_ALREADY_CURRENT: &str = "already_current";
pub const CODEX_AUTH_IMPORT_STATUS_REQUIRES_CONFIRMATION: &str = "requires_confirmation";

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CodexAuthImportTargetDto {
    pub file_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CodexAuthImportResultDto {
    pub status: &'static str,
    pub replaced_existing: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub confirmation: Option<String>,
}

impl From<CodexAuthImportOutcome> for CodexAuthImportResultDto {
    fn from(outcome: CodexAuthImportOutcome) -> Self {
        match outcome {
            CodexAuthImportOutcome::Imported { replaced_existing } => Self {
                status: CODEX_AUTH_IMPORT_STATUS_IMPORTED,
                replaced_existing,
                confirmation: None,
            },
            CodexAuthImportOutcome::AlreadyCurrent => Self {
                status: CODEX_AUTH_IMPORT_STATUS_ALREADY_CURRENT,
                replaced_existing: false,
                confirmation: None,
            },
            CodexAuthImportOutcome::RequiresConfirmation { confirmation } => Self {
                status: CODEX_AUTH_IMPORT_STATUS_REQUIRES_CONFIRMATION,
                replaced_existing: false,
                confirmation: Some(confirmation),
            },
        }
    }
}
