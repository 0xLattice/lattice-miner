use lattice_miner::*;
use std::time::Duration;

fn parse(json: &str) -> Result<MinerConfig, String> {
    serde_json::from_str::<MinerConfig>(json)
        .map_err(|e| e.to_string())?
        .validated()
}

#[test]
fn settings_are_checked_and_unknown_keys_refused() {
    let config = parse(
        r#"{"worker":"cpu","workers":2,"batchSize":5,"recipients":{"Nexus":"a","Nexus/Child":"b"},"minWork":{"Nexus":"2^32"},"minBlockIntervalSeconds":10,"templateTimeoutSeconds":30,"roundDeadlineMultiplier":3}"#,
    )
    .unwrap();
    assert_eq!(
        config.template_request().unwrap(),
        serde_json::json!({
            "recipients": [
                {"chainPath": ["Nexus"], "address": "a"},
                {"chainPath": ["Nexus", "Child"], "address": "b"},
            ],
            "minimumWork": [{"chainPath": ["Nexus"], "work": "0x100000000"}],
        })
    );
    assert_eq!(config.template_timeout(), Duration::from_secs(30));
    let defaults = parse("{}").unwrap();
    assert_eq!(defaults.workers(), 1);
    assert_eq!(defaults.batch_size(), 2_000_000_000);
    assert_eq!(defaults.template_timeout(), Duration::from_secs(60));
    assert_eq!(
        defaults.template_request().unwrap(),
        serde_json::json!({"recipients": []})
    );
    for bad in [
        r#"{"recipents":{}}"#,
        r#"{"rewards":[]}"#,
        r#"{"workers":0}"#,
        r#"{"batchSize":0}"#,
        r#"{"recipients":{"Child":"a"}}"#,
        r#"{"minWork":{"Nexus":"0"}}"#,
        r#"{"templateTimeoutSeconds":0}"#,
        r#"{"templateTimeoutSeconds":61}"#,
        r#"{"roundDeadlineMultiplier":0}"#,
        r#"{"worker":"/nonexistent/worker"}"#,
    ] {
        assert!(parse(bad).is_err(), "{bad}");
    }
}

#[test]
fn pacing_and_round_deadline() {
    let config = parse(r#"{"minBlockIntervalSeconds":10,"roundDeadlineMultiplier":3}"#).unwrap();
    assert_eq!(
        config.pacing_hold(Duration::from_secs(4)),
        Duration::from_secs(6)
    );
    assert_eq!(config.pacing_hold(Duration::from_secs(40)), Duration::ZERO);
    assert_eq!(
        parse("{}").unwrap().pacing_hold(Duration::ZERO),
        Duration::ZERO
    );
    // The allowance until a round completes, and never less.
    assert_eq!(config.round_deadline(Duration::ZERO), FIRST_ROUND_ALLOWANCE);
    assert_eq!(
        config.round_deadline(Duration::from_secs(100)),
        FIRST_ROUND_ALLOWANCE
    );
    assert_eq!(
        config.round_deadline(Duration::from_secs(300)),
        Duration::from_secs(900)
    );
    // A slow round counts at most the allowance.
    assert_eq!(
        config.round_deadline(Duration::from_secs(5000)),
        Duration::from_secs(1800)
    );
    let huge = parse(&format!(r#"{{"roundDeadlineMultiplier":{}}}"#, u64::MAX)).unwrap();
    assert!(huge.round_deadline(Duration::from_secs(600)) >= FIRST_ROUND_ALLOWANCE);
}
