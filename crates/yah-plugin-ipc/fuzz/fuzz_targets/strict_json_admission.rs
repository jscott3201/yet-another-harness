#![no_main]

mod common;

libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    if data.len() > common::MAX_INPUT_BYTES {
        return;
    }
    match yah_plugin_ipc::strict::parse(data) {
        Ok(value) => {
            assert_no_unsafe_numbers(&value);
            // Admitted bytes must mean the same thing on the second read:
            // this is the property a second SDK's decoder depends on.
            let rewritten = serde_json::to_string(&value).expect("admitted value serializes");
            let reparsed =
                yah_plugin_ipc::strict::parse(rewritten.as_bytes()).expect("rewrite re-admits");
            assert_eq!(reparsed, value, "rewrite changed the value");
        }
        Err(
            yah_plugin_ipc::strict::StrictJsonError::Syntax(_)
            | yah_plugin_ipc::strict::StrictJsonError::DuplicateMember(_)
            | yah_plugin_ipc::strict::StrictJsonError::UnsafeInteger(_),
        ) => {}
    }
});

/// Every integer in an admitted value is within the I-JSON safe range and
/// every float is finite. The walk is bounded by the parser's own
/// recursion limit, so it cannot overflow the stack on deep input.
fn assert_no_unsafe_numbers(value: &serde_json::Value) {
    match value {
        serde_json::Value::Number(number) => {
            if let Some(int) = number.as_i64() {
                assert!(
                    int >= -(SAFE_MAX as i64) && int <= SAFE_MAX as i64,
                    "admitted {int}, outside the safe range"
                );
            } else if let Some(uint) = number.as_u64() {
                assert!(uint <= SAFE_MAX, "admitted {uint}, outside the safe range");
            } else {
                let float = number.as_f64().expect("a number is i64, u64, or f64");
                assert!(float.is_finite(), "admitted a non-finite float");
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                assert_no_unsafe_numbers(item);
            }
        }
        serde_json::Value::Object(map) => {
            for item in map.values() {
                assert_no_unsafe_numbers(item);
            }
        }
        _ => {}
    }
}

const SAFE_MAX: u64 = (1 << 53) - 1;
