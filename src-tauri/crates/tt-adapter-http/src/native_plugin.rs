use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::{Method, Url};

use tt_contracts::native_plugin::{NativePluginHttpRequest, NativePluginHttpResponse};
use tt_domain::errors::DomainError;
use tt_ports::native_plugin::NativePluginHttpGateway;

use crate::{HttpClientPool, HttpClientProfile};

const MAX_BODY_BYTES: usize = 8 * 1024 * 1024;

pub struct ReqwestNativePluginHttpGateway {
    clients: Arc<HttpClientPool>,
}

impl ReqwestNativePluginHttpGateway {
    pub fn new(clients: Arc<HttpClientPool>) -> Self {
        Self { clients }
    }
}

#[async_trait]
impl NativePluginHttpGateway for ReqwestNativePluginHttpGateway {
    async fn send(
        &self,
        allowed_origins: &[String],
        request: NativePluginHttpRequest,
    ) -> Result<NativePluginHttpResponse, DomainError> {
        let url = validate_allowed_url(allowed_origins, &request.url)?;
        let method = parse_method(&request.method)?;
        let headers = parse_headers(request.headers)?;
        let body = parse_body(request.body, request.body_base64)?;

        let client = self.clients.client(HttpClientProfile::NativePlugin)?;
        let mut builder = client.request(method, url).headers(headers);
        if let Some(body) = body {
            builder = builder.body(body);
        }
        let mut response = builder.send().await.map_err(|error| {
            DomainError::Transient(format!("Native plugin HTTP request failed: {error}"))
        })?;
        let status = response.status().as_u16();
        let response_headers = serialize_headers(response.headers());
        if response
            .content_length()
            .is_some_and(|length| length > MAX_BODY_BYTES as u64)
        {
            return Err(DomainError::InvalidData(format!(
                "Native plugin HTTP response exceeds the {MAX_BODY_BYTES}-byte limit"
            )));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|error| {
            DomainError::Transient(format!(
                "Failed to read native plugin HTTP response: {error}"
            ))
        })? {
            if bytes.len().saturating_add(chunk.len()) > MAX_BODY_BYTES {
                return Err(DomainError::InvalidData(format!(
                    "Native plugin HTTP response exceeds the {MAX_BODY_BYTES}-byte limit"
                )));
            }
            bytes.extend_from_slice(&chunk);
        }

        let (body, body_base64) = match String::from_utf8(bytes) {
            Ok(text) => (Some(text), None),
            Err(error) => (None, Some(BASE64_STANDARD.encode(error.into_bytes()))),
        };
        Ok(NativePluginHttpResponse {
            status,
            headers: response_headers,
            body,
            body_base64,
        })
    }
}

fn validate_allowed_url(allowed_origins: &[String], request_url: &str) -> Result<Url, DomainError> {
    let allowed = allowed_origins
        .iter()
        .map(|origin| canonical_manifest_origin(origin))
        .collect::<Result<HashSet<_>, _>>()?;
    let url = Url::parse(request_url).map_err(|error| {
        DomainError::InvalidData(format!("Invalid native plugin request URL: {error}"))
    })?;
    if !matches!(url.scheme(), "http" | "https") || !url.has_host() {
        return Err(DomainError::InvalidData(
            "Native plugin request URL must be absolute HTTP or HTTPS".to_string(),
        ));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(DomainError::InvalidData(
            "Native plugin request URL must not contain credentials".to_string(),
        ));
    }
    let origin = url.origin().ascii_serialization();
    if !allowed.contains(&origin) {
        return Err(DomainError::AuthenticationError(format!(
            "Native plugin manifest does not permit HTTP origin `{origin}`"
        )));
    }
    Ok(url)
}

fn canonical_manifest_origin(origin: &str) -> Result<String, DomainError> {
    let url = Url::parse(origin).map_err(|error| {
        DomainError::InvalidData(format!(
            "Invalid native plugin HTTP origin `{origin}`: {error}"
        ))
    })?;
    if !matches!(url.scheme(), "http" | "https")
        || !url.has_host()
        || !url.username().is_empty()
        || url.password().is_some()
        || !matches!(url.path(), "" | "/")
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(DomainError::InvalidData(format!(
            "Native plugin HTTP permission `{origin}` must be an exact HTTP or HTTPS origin"
        )));
    }
    Ok(url.origin().ascii_serialization())
}

fn parse_method(method: &str) -> Result<Method, DomainError> {
    let method = Method::from_bytes(method.trim().as_bytes()).map_err(|error| {
        DomainError::InvalidData(format!("Invalid native plugin HTTP method: {error}"))
    })?;
    if matches!(method, Method::CONNECT | Method::TRACE) {
        return Err(DomainError::InvalidData(format!(
            "Native plugin HTTP method {method} is not allowed"
        )));
    }
    Ok(method)
}

fn parse_headers(headers: BTreeMap<String, String>) -> Result<HeaderMap, DomainError> {
    let mut parsed = HeaderMap::new();
    for (name, value) in headers {
        let lower_name = name.to_ascii_lowercase();
        if matches!(
            lower_name.as_str(),
            "host"
                | "content-length"
                | "connection"
                | "transfer-encoding"
                | "upgrade"
                | "proxy-authenticate"
                | "proxy-authorization"
        ) {
            return Err(DomainError::InvalidData(format!(
                "Native plugin may not set HTTP header `{name}`"
            )));
        }
        let name = HeaderName::from_bytes(name.as_bytes()).map_err(|error| {
            DomainError::InvalidData(format!("Invalid native plugin HTTP header name: {error}"))
        })?;
        let value = HeaderValue::from_str(&value).map_err(|error| {
            DomainError::InvalidData(format!("Invalid native plugin HTTP header value: {error}"))
        })?;
        parsed.insert(name, value);
    }
    Ok(parsed)
}

fn parse_body(
    body: Option<String>,
    body_base64: Option<String>,
) -> Result<Option<Vec<u8>>, DomainError> {
    let bytes = match (body, body_base64) {
        (Some(_), Some(_)) => {
            return Err(DomainError::InvalidData(
                "Native plugin HTTP request must provide either body or bodyBase64, not both"
                    .to_string(),
            ));
        }
        (Some(body), None) => Some(body.into_bytes()),
        (None, Some(encoded)) => Some(BASE64_STANDARD.decode(encoded).map_err(|error| {
            DomainError::InvalidData(format!("Invalid native plugin bodyBase64: {error}"))
        })?),
        (None, None) => None,
    };
    if bytes
        .as_ref()
        .is_some_and(|body| body.len() > MAX_BODY_BYTES)
    {
        return Err(DomainError::InvalidData(format!(
            "Native plugin HTTP request body exceeds the {MAX_BODY_BYTES}-byte limit"
        )));
    }
    Ok(bytes)
}

fn serialize_headers(headers: &HeaderMap) -> BTreeMap<String, String> {
    headers
        .iter()
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|value| (name.as_str().to_string(), value.to_string()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::validate_allowed_url;

    #[test]
    fn requires_exact_manifest_origin() {
        let allowed = vec!["https://example.com".to_string()];
        assert!(validate_allowed_url(&allowed, "https://example.com/api/cards").is_ok());
        assert!(validate_allowed_url(&allowed, "https://sub.example.com/api/cards").is_err());
        assert!(validate_allowed_url(&allowed, "http://example.com/api/cards").is_err());
    }

    #[test]
    fn rejects_manifest_permissions_with_paths() {
        let allowed = vec!["https://example.com/api".to_string()];
        assert!(validate_allowed_url(&allowed, "https://example.com/api").is_err());
    }
}
