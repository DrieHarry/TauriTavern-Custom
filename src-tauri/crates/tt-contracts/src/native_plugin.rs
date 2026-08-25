use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

pub const NATIVE_PLUGIN_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NativePluginHttpPermissions {
    #[serde(default)]
    pub origins: Vec<String>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NativePluginPermissions {
    #[serde(default)]
    pub http: NativePluginHttpPermissions,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NativePluginManifest {
    pub schema_version: u32,
    pub id: String,
    pub name: String,
    pub version: String,
    pub entry: String,
    #[serde(default)]
    pub permissions: NativePluginPermissions,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum NativePluginScope {
    Local,
    Global,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct NativePluginDescriptor {
    pub id: String,
    pub name: String,
    pub version: String,
    pub extension_name: String,
    pub scope: NativePluginScope,
    pub permissions: NativePluginPermissions,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NativePluginCallDto {
    pub plugin_id: String,
    pub operation: String,
    #[serde(default)]
    pub input: serde_json::Value,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NativePluginIdDto {
    pub plugin_id: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NativePluginHttpRequest {
    pub method: String,
    pub url: String,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    #[serde(default)]
    pub body: Option<String>,
    #[serde(default)]
    pub body_base64: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct NativePluginHttpResponse {
    pub status: u16,
    pub headers: BTreeMap<String, String>,
    pub body: Option<String>,
    pub body_base64: Option<String>,
}
