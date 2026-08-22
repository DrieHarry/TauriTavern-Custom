use reqwest::header::{ACCEPT, CONTENT_TYPE};
use serde_json::{Map, Value, json};

use super::{bytes_response, send_with_retry, upstream_error_response};
use tt_domain::errors::DomainError;
use tt_ports::repositories::tts_repository::{
    ElectronHubTtsRequest, OpenAiTtsRequest, TtsRouteResponse,
};

const OPENAI_TTS_URL: &str = "https://api.openai.com/v1/audio/speech";
const ELECTRONHUB_TTS_URL: &str = "https://api.electronhub.ai/v1/audio/speech";
const ELECTRONHUB_MODELS_URL: &str = "https://api.electronhub.ai/v1/models";
const CHUTES_TTS_URL: &str = "https://chutes-kokoro.chutes.ai/speak";

pub(super) async fn handle_openai(
    client: reqwest::Client,
    request: OpenAiTtsRequest,
) -> Result<TtsRouteResponse, DomainError> {
    match request {
        OpenAiTtsRequest::Generate {
            api_key,
            text,
            voice,
            model,
            speed,
            instructions,
        } => generate_openai(client, api_key, text, voice, model, speed, instructions).await,
        OpenAiTtsRequest::CompatibleGenerate {
            api_key,
            endpoint,
            input,
            voice,
            model,
            response_format,
            speed,
            instructions,
        } => {
            let is_mistral = is_mistral_tts_request(&endpoint, &model);
            let payload = compatible_tts_payload(
                input,
                voice,
                model,
                &response_format,
                speed,
                instructions,
                is_mistral,
            );
            let response = send_with_retry("OpenAI-compatible TTS request", || {
                client
                    .post(endpoint.clone())
                    .bearer_auth(api_key.as_deref().unwrap_or_default())
                    .header(ACCEPT, "*/*")
                    .header(CONTENT_TYPE, "application/json")
                    .json(&payload)
            })
            .await?;

            if !response.status().is_success() {
                return upstream_error_response(response, "OpenAI-compatible TTS request failed")
                    .await;
            }

            let content_type = response
                .headers()
                .get(CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_string();

            if content_type.contains("application/json") {
                let json_data: Value = response.json().await.map_err(|error| {
                    DomainError::InternalError(format!(
                        "OpenAI-compatible TTS JSON response read failed: {error}"
                    ))
                })?;
                let audio_b64 = json_data
                    .get("audio_data")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        DomainError::InvalidData(
                            "OpenAI-compatible TTS JSON response contains no audio_data"
                                .to_string(),
                        )
                    })?;
                let bytes = base64::Engine::decode(
                    &base64::engine::general_purpose::STANDARD,
                    audio_b64.as_bytes(),
                )
                .map_err(|error| {
                    DomainError::InvalidData(format!("Failed to decode base64 audio_data: {error}"))
                })?;
                Ok(TtsRouteResponse::bytes(
                    200,
                    get_audio_content_type(&response_format),
                    bytes,
                ))
            } else {
                let bytes = response.bytes().await.map_err(|error| {
                    DomainError::InternalError(format!(
                        "OpenAI-compatible TTS response read failed: {error}"
                    ))
                })?;
                let mime = if !content_type.is_empty() {
                    content_type.as_str()
                } else {
                    get_audio_content_type(&response_format)
                };
                Ok(TtsRouteResponse::bytes(200, mime, bytes.to_vec()))
            }
        }
    }
}

fn compatible_tts_payload(
    input: String,
    voice: String,
    model: String,
    response_format: &str,
    speed: f64,
    instructions: Option<String>,
    is_mistral: bool,
) -> Value {
    let mut payload = json!({
        "input": input,
        "response_format": response_format,
        "model": model,
    });
    let voice_field = if is_mistral { "voice_id" } else { "voice" };
    payload[voice_field] = Value::String(voice);
    if !is_mistral {
        payload["speed"] = json!(speed);
    }
    if let Some(instructions) = instructions {
        payload["instructions"] = Value::String(instructions);
    }
    payload
}

pub(crate) fn is_mistral_tts_request(endpoint: &url::Url, model: &str) -> bool {
    let hostname = endpoint.host_str().unwrap_or("").to_ascii_lowercase();
    let is_mistral_host = hostname == "api.mistral.ai" || hostname.ends_with(".mistral.ai");

    let normalized_model = model.trim().to_ascii_lowercase();
    let is_mistral_model =
        normalized_model.starts_with("voxtral-") && normalized_model.contains("-tts-");

    is_mistral_host || is_mistral_model
}

pub(crate) fn get_audio_content_type(format: &str) -> &'static str {
    match format.trim().to_ascii_lowercase().as_str() {
        "aac" => "audio/aac",
        "flac" => "audio/flac",
        "mp3" => "audio/mpeg",
        "opus" => "audio/ogg",
        "wav" => "audio/wav",
        _ => "application/octet-stream",
    }
}

async fn generate_openai(
    client: reqwest::Client,
    api_key: String,
    text: String,
    voice: String,
    model: String,
    speed: f64,
    instructions: Option<String>,
) -> Result<TtsRouteResponse, DomainError> {
    let mut payload = json!({
        "input": text,
        "response_format": "mp3",
        "voice": voice,
        "speed": speed,
        "model": model,
    });
    if let Some(instructions) = instructions {
        payload["instructions"] = Value::String(instructions);
    }

    let response = send_with_retry("OpenAI TTS request", || {
        client
            .post(OPENAI_TTS_URL)
            .bearer_auth(&api_key)
            .header(ACCEPT, "*/*")
            .header(CONTENT_TYPE, "application/json")
            .json(&payload)
    })
    .await?;
    bytes_response(response, "OpenAI TTS request", "audio/mpeg", false).await
}

pub(super) async fn handle_electronhub(
    client: reqwest::Client,
    request: ElectronHubTtsRequest,
) -> Result<TtsRouteResponse, DomainError> {
    match request {
        ElectronHubTtsRequest::Models { api_key } => {
            let response = send_with_retry("ElectronHub model list request", || {
                client
                    .get(ELECTRONHUB_MODELS_URL)
                    .bearer_auth(&api_key)
                    .header(ACCEPT, "application/json")
            })
            .await?;
            if !response.status().is_success() {
                return upstream_error_response(response, "ElectronHub model list request failed")
                    .await;
            }
            let payload: Value = response.json().await.map_err(|error| {
                DomainError::InternalError(format!(
                    "ElectronHub model list response read failed: {error}"
                ))
            })?;
            let models = payload
                .get("data")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            Ok(TtsRouteResponse::bytes(
                200,
                "application/json; charset=utf-8",
                serde_json::to_vec(&models).map_err(|error| {
                    DomainError::InternalError(format!(
                        "ElectronHub model list response encode failed: {error}"
                    ))
                })?,
            ))
        }
        ElectronHubTtsRequest::Generate {
            api_key,
            mut payload,
        } => {
            let Some(payload) = payload.as_object_mut() else {
                return Err(DomainError::InvalidData(
                    "ElectronHub TTS payload must be an object".to_string(),
                ));
            };
            normalize_electronhub_payload(payload);
            let response = send_with_retry("ElectronHub TTS request", || {
                client
                    .post(ELECTRONHUB_TTS_URL)
                    .bearer_auth(&api_key)
                    .header(ACCEPT, "*/*")
                    .header(CONTENT_TYPE, "application/json")
                    .json(&payload)
            })
            .await?;
            bytes_response(response, "ElectronHub TTS request", "audio/mpeg", true).await
        }
    }
}

fn normalize_electronhub_payload(payload: &mut Map<String, Value>) {
    if payload.get("speed").is_none_or(Value::is_null) {
        payload.insert("speed".to_string(), json!(1));
    }
    if !payload
        .get("model")
        .and_then(Value::as_str)
        .is_some_and(|model| !model.is_empty())
    {
        payload.insert("model".to_string(), json!("tts-1"));
    }
    payload.insert("response_format".to_string(), json!("mp3"));
}

pub(super) async fn generate_chutes(
    client: reqwest::Client,
    api_key: String,
    input: String,
    voice: String,
    speed: f64,
) -> Result<TtsRouteResponse, DomainError> {
    let payload = json!({
        "text": input,
        "voice": voice,
        "speed": speed,
    });
    let response = send_with_retry("Chutes TTS request", || {
        client
            .post(CHUTES_TTS_URL)
            .bearer_auth(&api_key)
            .header(ACCEPT, "*/*")
            .header(CONTENT_TYPE, "application/json")
            .json(&payload)
    })
    .await?;
    bytes_response(response, "Chutes TTS request", "audio/mpeg", true).await
}

#[cfg(test)]
mod tests {
    use serde_json::{Map, json};

    use super::normalize_electronhub_payload;

    #[test]
    fn electronhub_preserves_dynamic_parameters_and_sets_contract_fields() {
        let mut payload = Map::from_iter([
            ("model".to_string(), json!("custom-model")),
            ("custom_parameter".to_string(), json!("selected")),
        ]);
        normalize_electronhub_payload(&mut payload);

        assert_eq!(payload["model"], "custom-model");
        assert_eq!(payload["speed"], 1);
        assert_eq!(payload["response_format"], "mp3");
        assert_eq!(payload["custom_parameter"], "selected");
    }

    #[test]
    fn test_is_mistral_tts_request() {
        let mistral_url: url::Url = "https://api.mistral.ai/v1/audio/speech".parse().unwrap();
        assert!(super::is_mistral_tts_request(&mistral_url, "tts-1"));

        let gateway_url: url::Url = "https://custom-gateway.local/v1/audio/speech"
            .parse()
            .unwrap();
        assert!(super::is_mistral_tts_request(
            &gateway_url,
            "voxtral-mini-tts-v1"
        ));
        assert!(!super::is_mistral_tts_request(&gateway_url, "tts-1"));
    }

    #[test]
    fn compatible_tts_payload_uses_mistral_voice_id_without_changing_other_providers() {
        let mistral = super::compatible_tts_payload(
            "hello".to_string(),
            "voice-123".to_string(),
            "voxtral-mini-tts-v1".to_string(),
            "mp3",
            1.25,
            None,
            true,
        );
        assert_eq!(mistral["voice_id"], "voice-123");
        assert!(mistral.get("voice").is_none());
        assert!(mistral.get("speed").is_none());

        let compatible = super::compatible_tts_payload(
            "hello".to_string(),
            "alloy".to_string(),
            "tts-1".to_string(),
            "mp3",
            1.25,
            Some("Warmly".to_string()),
            false,
        );
        assert_eq!(compatible["voice"], "alloy");
        assert!(compatible.get("voice_id").is_none());
        assert_eq!(compatible["speed"], 1.25);
        assert_eq!(compatible["instructions"], "Warmly");
    }

    #[test]
    fn test_get_audio_content_type() {
        assert_eq!(super::get_audio_content_type("mp3"), "audio/mpeg");
        assert_eq!(super::get_audio_content_type("flac"), "audio/flac");
        assert_eq!(super::get_audio_content_type("wav"), "audio/wav");
        assert_eq!(super::get_audio_content_type("opus"), "audio/ogg");
        assert_eq!(super::get_audio_content_type("aac"), "audio/aac");
        assert_eq!(
            super::get_audio_content_type("raw"),
            "application/octet-stream"
        );
    }
}
