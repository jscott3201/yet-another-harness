//! Strict JSON admission at the frame boundary.
//!
//! Two rules decide whether two SDK decoders read the same bytes as the
//! same value: duplicate member names are refused rather than last-wins
//! resolved, and integers outside the I-JSON safe range are refused rather
//! than rounded — including literals too wide for a 64-bit lane, which the
//! raw token scan must catch before the parser rounds them to floats.
//! This file pins both rules at their exact boundaries, plus the shapes a
//! hostile peer sends: invalid UTF-8, malformed escapes, trailing bytes,
//! and nesting deep enough to find the recursion limit.

use yah_plugin_ipc::MAX_ERROR_DETAIL_CHARS;
use yah_plugin_ipc::session::{HostSession, SessionEvent};
use yah_plugin_ipc::strict::{self, StrictJsonError};
use yah_plugin_ipc::types::WireErrorKind;

/// Parse a literal and classify the refusal, if any.
fn classify(text: &str) -> Result<serde_json::Value, StrictJsonError> {
    strict::parse(text.as_bytes())
}

fn refused_integer(text: &str) -> String {
    match classify(text) {
        Err(StrictJsonError::UnsafeInteger(token)) => token,
        other => panic!("expected an unsafe-integer refusal for {text:?}, got {other:?}"),
    }
}

fn refused_duplicate(text: &str) -> String {
    match classify(text) {
        Err(StrictJsonError::DuplicateMember(name)) => name,
        other => panic!("expected a duplicate-member refusal for {text:?}, got {other:?}"),
    }
}

fn refused_syntax(text: &[u8]) -> String {
    match strict::parse(text) {
        Err(StrictJsonError::Syntax(detail)) => detail,
        other => panic!("expected a syntax refusal, got {other:?}"),
    }
}

#[test]
fn the_safe_integer_boundaries_are_admitted_and_their_neighbors_refused() {
    let safe_max: u64 = (1 << 53) - 1;
    let safe_min: i64 = -((1 << 53) - 1);
    for (text, expected) in [
        (safe_max.to_string(), serde_json::json!(safe_max)),
        (safe_min.to_string(), serde_json::json!(safe_min)),
    ] {
        assert_eq!(classify(&text), Ok(expected), "{text} is safe");
    }
    for text in [
        (safe_max + 1).to_string(),
        (safe_min - 1).to_string(),
        ((1u64 << 53) + 1).to_string(),
        (-(1i64 << 53) - 1).to_string(),
    ] {
        let token = refused_integer(&text);
        assert_eq!(token, text, "the refusal names the offending token");
    }
}

#[test]
fn literals_too_wide_for_a_native_integer_are_refused_on_the_raw_token() {
    // Every token here overflows u64 or i64 parsing, so serde's own parser
    // would round it to a float and lose the exact value. The raw scan is
    // the only thing standing between these and a silently rounded frame.
    for text in [
        "99999999999999999999",
        "-99999999999999999999",
        "18446744073709551616",
        "-18446744073709551616",
        "1000000000000000000000000000001",
    ] {
        let token = refused_integer(text);
        assert_eq!(token, *text);
    }
    // The refusal is raised before the parser runs: the same digits as a
    // float suffix, a string, or a member name are not integer tokens.
    for text in [
        r#"{"a":"99999999999999999999"}"#,
        r#"{"99999999999999999999":1}"#,
        "[1e300, -2.5, 0.1, 1E+2]",
        r#""99999999999999999999""#,
    ] {
        assert!(classify(text).is_ok(), "{text} is not an integer literal");
    }
}

#[test]
fn float_and_exponent_forms_are_admitted_whole() {
    for text in ["1e300", "-2.5", "0.1", "1E+2", "2.5e-10", "-0.0", "1.0"] {
        assert!(classify(text).is_ok(), "{text} is a legal float");
    }
}

#[test]
fn digit_runs_inside_strings_are_never_integer_tokens() {
    // A quote hides digits from the scan; an escaped quote must not end
    // that hiding early.
    for text in [
        r#""99999999999999999999""#,
        r#""a\"9999999999999999999""#,
        r#""\\\\999999999999999999""#,
        r#"{"m":"1e999999999999999999"}"#,
    ] {
        assert!(classify(text).is_ok(), "{text} must pass the raw scan");
    }
}

#[test]
fn duplicate_members_are_refused_at_every_nesting_level() {
    assert_eq!(refused_duplicate(r#"{"a":1,"a":2}"#), "a");
    assert_eq!(
        refused_duplicate(r#"{"outer":{"a":1,"b":2,"a":3}}"#),
        "a",
        "nested object"
    );
    assert_eq!(
        refused_duplicate(r#"[{"a":1},{"a":2,"a":3}]"#),
        "a",
        "object inside an array"
    );
    assert_eq!(
        refused_duplicate(r#"{"frame":"goodbye","reason":"a","reason":"b"}"#),
        "reason",
        "the exact shape the session fixtures refuse"
    );
}

#[test]
fn escaped_member_names_are_unescaped_before_the_duplicate_check() {
    // \u0061 is 'a': the two spellings name one member, so this is a
    // duplicate no matter how it is written on the wire.
    assert_eq!(refused_duplicate(r#"{"\u0061":1,"a":2}"#), "a");
    // Distinct names, one escaped, are not duplicates.
    assert!(classify(r#"{"\u0061":1,"b":2}"#).is_ok());
}

#[test]
fn hostile_byte_shapes_are_syntax_refusals_never_panics() {
    assert!(!refused_syntax(b"").is_empty(), "empty input");
    assert!(!refused_syntax(b"not json").is_empty(), "not JSON");
    assert!(!refused_syntax(b"{} {}").is_empty(), "trailing bytes");
    assert!(
        !refused_syntax(br#"{"a":"x"} tail"#).is_empty(),
        "trailing value"
    );
    assert!(
        !refused_syntax(br#"{"a":"\x"}"#).is_empty(),
        "malformed escape"
    );
    assert!(!refused_syntax(br#"{"a":}"#).is_empty(), "missing value");
    assert!(!refused_syntax(b"\xFF\xFE").is_empty(), "invalid UTF-8");
    assert!(
        !refused_syntax(b"\"unterminated").is_empty(),
        "unterminated string"
    );
    assert!(!refused_syntax(b"-").is_empty(), "lone minus");
    assert!(!refused_syntax(b"-.5").is_empty(), "leading-dot float");
    assert!(!refused_syntax(b"1e").is_empty(), "dangling exponent");
    assert!(!refused_syntax(b"01").is_empty(), "leading zero");
}

#[test]
fn nesting_past_the_parser_limit_is_a_refusal_not_a_crash() {
    // serde_json's default recursion limit is 128. Two hundred thousand
    // opens would blow the stack in an unprotected parser; here it is one
    // bounded syntax error.
    let deep = "[".repeat(200_000);
    let detail = refused_syntax(deep.as_bytes());
    assert!(
        detail.contains("recursion limit") || detail.contains("limit"),
        "the refusal should name the recursion limit: {detail}"
    );
    // Just under the limit parses fine.
    let ok = format!("{}1{}", "[".repeat(100), "]".repeat(100));
    assert!(classify(&ok).is_ok());
}

#[test]
fn float_literals_parse_to_the_bit_patterns_node_and_cpython_produce() {
    // An independent IEEE-754 oracle, not serde grading its own homework:
    // each expected value is the raw f64 bit pattern that Node 26.7.0's
    // `JSON.parse` and CPython 3.14.7's `json.loads` produce for the
    // literal, derived from both runtimes on 2026-08-20 and cross-checked
    // to agree before being pinned here. Correct rounding is defined by
    // IEEE-754, so the patterns are runtime facts, not implementation
    // choices. This pins the boundary literals; proving equivalence across
    // whole corpora of real traffic is the process-SDK conformance work
    // (M4-PR07/M4-PR10), not this test.
    for (literal, bits) in [
        ("2.5e-30", 0x39c9_5a5e_fea6_b347_u64),
        ("2.5e-10", 0x3df1_2e0b_e826_d695),
        ("1e300", 0x7e37_e43c_8800_759c),
        ("0.1", 0x3fb9_9999_9999_999a),
        ("5e-324", 0x0000_0000_0000_0001),
        ("1.7976931348623157e308", 0x7fef_ffff_ffff_ffff),
        ("1e-300", 0x01a5_6e1f_c2f8_f359),
        ("3.14159e-5", 0x3f00_7892_1ac6_c11f),
    ] {
        let value = classify(literal).expect("admitted");
        let parsed = value.as_f64().expect("a float literal");
        assert_eq!(
            parsed.to_bits(),
            bits,
            "{literal} must read as the double Node and CPython produce"
        );
    }
}

#[test]
fn float_literals_round_trip_through_their_own_serialization() {
    // Found by the M4-01 fuzzer: without serde_json's `float_roundtrip`
    // feature the parser can land one ULP off the correctly rounded
    // double, so re-reading a re-serialized value changed it — a
    // divergence from JavaScript's and CPython's correctly rounded
    // decoders, on ordinary literals.
    for text in [
        "2.5e-30",
        "2.5e-10",
        "1e300",
        "0.1",
        "5e-324",
        "1.7976931348623157e308",
    ] {
        let value = classify(text).expect("admitted");
        let rewritten = serde_json::to_string(&value).expect("serializes");
        assert_eq!(
            classify(&rewritten),
            Ok(value.clone()),
            "{text} did not survive its own serialization"
        );
    }
}

#[test]
fn an_admitted_value_round_trips_through_its_own_serialization() {
    // Whatever the strict layer admits must mean the same thing when it is
    // written back out and read again — the property a second SDK's
    // decoder depends on.
    let text = r#"{"a":[1,-2,3.5,"x",null,true,{"b":{"c":""}}]}"#;
    let value = classify(text).expect("admitted");
    let rewritten = serde_json::to_string(&value).expect("serializes");
    assert_eq!(classify(&rewritten), Ok(value));
}

#[test]
fn a_hostile_member_name_cannot_bloat_the_session_diagnostic() {
    // The strict layer's refusal carries the duplicate name verbatim; the
    // session clips what it reports so a 1 MiB frame of one member name
    // cannot become a 1 MiB diagnostic.
    let name = "k".repeat(400_000);
    let text = format!(r#"{{"{name}":1,"{name}":2}}"#);
    let error = classify(&text);
    assert!(matches!(error, Err(StrictJsonError::DuplicateMember(_))));
    let mut session = HostSession::new(Default::default());
    let framed = yah_plugin_ipc::frame::encode(text.as_bytes());
    session.feed(&framed);
    let detail = session
        .drain_events()
        .into_iter()
        .find_map(|event| match event {
            SessionEvent::Fatal { kind, detail } => {
                assert_eq!(kind, WireErrorKind::InvalidFrame);
                Some(detail)
            }
            _ => None,
        })
        .expect("a duplicate member is fatal");
    assert!(
        detail.chars().count() <= MAX_ERROR_DETAIL_CHARS,
        "the diagnostic must be clipped, not echoed"
    );
}
