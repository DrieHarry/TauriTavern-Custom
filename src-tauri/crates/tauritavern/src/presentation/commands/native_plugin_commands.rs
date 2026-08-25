use std::sync::Arc;

use serde_json::Value;
use tauri::State;

use tt_contracts::native_plugin::{NativePluginCallDto, NativePluginDescriptor, NativePluginIdDto};

use crate::app::AppState;
use crate::presentation::commands::helpers::{log_command, map_command_error};
use crate::presentation::errors::CommandError;

#[tauri::command]
pub async fn list_native_plugins(
    app_state: State<'_, Arc<AppState>>,
) -> Result<Vec<NativePluginDescriptor>, CommandError> {
    log_command("list_native_plugins");
    app_state
        .services
        .native_plugin_service
        .list()
        .await
        .map_err(map_command_error("Failed to list native plugins"))
}

#[tauri::command]
pub async fn call_native_plugin(
    dto: NativePluginCallDto,
    app_state: State<'_, Arc<AppState>>,
) -> Result<Value, CommandError> {
    log_command(format!(
        "call_native_plugin {}:{}",
        dto.plugin_id, dto.operation
    ));
    app_state
        .services
        .native_plugin_service
        .call(&dto.plugin_id, &dto.operation, dto.input)
        .await
        .map_err(map_command_error("Native plugin call failed"))
}

#[tauri::command]
pub async fn deactivate_native_plugin(
    dto: NativePluginIdDto,
    app_state: State<'_, Arc<AppState>>,
) -> Result<(), CommandError> {
    log_command(format!("deactivate_native_plugin {}", dto.plugin_id));
    app_state
        .services
        .native_plugin_service
        .deactivate(&dto.plugin_id)
        .await
        .map_err(map_command_error("Failed to deactivate native plugin"))
}
