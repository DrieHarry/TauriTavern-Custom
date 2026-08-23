use std::sync::Arc;

use tauri::State;
use tt_application::dto::codex_auth_dto::{CodexAuthImportResultDto, CodexAuthImportTargetDto};

use crate::app::AppState;
use crate::presentation::commands::helpers::{log_command, map_command_error};
use crate::presentation::errors::CommandError;

#[tauri::command]
pub async fn prepare_codex_auth_import(
    app_state: State<'_, Arc<AppState>>,
) -> Result<CodexAuthImportTargetDto, CommandError> {
    log_command("prepare_codex_auth_import");

    app_state
        .services
        .codex_auth_service
        .prepare_import_staging_path()
        .await
        .map_err(map_command_error("Failed to prepare Codex auth import"))
}

#[tauri::command]
pub async fn import_codex_auth(
    file_path: String,
    expected_existing: Option<String>,
    app_state: State<'_, Arc<AppState>>,
) -> Result<CodexAuthImportResultDto, CommandError> {
    log_command("import_codex_auth");

    app_state
        .services
        .codex_auth_service
        .import_auth_file(&file_path, expected_existing.as_deref())
        .await
        .map_err(map_command_error("Failed to import Codex auth"))
}

#[tauri::command]
pub async fn discard_codex_auth_import(
    file_path: String,
    app_state: State<'_, Arc<AppState>>,
) -> Result<(), CommandError> {
    log_command("discard_codex_auth_import");

    app_state
        .services
        .codex_auth_service
        .discard_import_staging_path(&file_path)
        .await
        .map_err(map_command_error(
            "Failed to discard staged Codex auth import",
        ))
}
