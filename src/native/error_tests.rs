use crate::{
    native::{self, NativeEvent},
    protocol::{ErrorCode, RuntimeEventKind},
};
use serde::Deserialize;

#[derive(Deserialize)]
struct FailureWire {
    message: String,
    code: Option<String>,
}

#[test]
fn native_failures_preserve_every_runtime_error_code_on_the_wire() {
    for (code, expected) in [
        (ErrorCode::InvalidRequest, "invalid_request"),
        (ErrorCode::AuthenticationRequired, "authentication_required"),
        (ErrorCode::AuthenticationFailed, "authentication_failed"),
        (ErrorCode::ProviderUnavailable, "provider_unavailable"),
        (ErrorCode::CoordinatorUnavailable, "coordinator_unavailable"),
        (ErrorCode::StorageUnavailable, "storage_unavailable"),
        (ErrorCode::Internal, "internal"),
    ] {
        let projected = native::project(&RuntimeEventKind::Failure {
            code,
            message: "Synthetic failure".into(),
        })
        .unwrap();
        let wire: FailureWire =
            serde_json::from_slice(&serde_json::to_vec(&projected).unwrap()).unwrap();
        assert_eq!(wire.code.as_deref(), Some(expected));
        assert_eq!(wire.message, "Synthetic failure");
    }
}

#[test]
fn legacy_native_failures_remain_decodable_without_inventing_a_code() {
    let legacy: NativeEvent =
        serde_json::from_str(r#"{"type":"failure","message":"Legacy failure"}"#).unwrap();
    let wire: FailureWire = serde_json::from_slice(&serde_json::to_vec(&legacy).unwrap()).unwrap();
    assert_eq!(wire.message, "Legacy failure");
    assert!(wire.code.is_none());
}
