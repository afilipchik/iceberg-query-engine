//! Deterministic test producer for the Pulsar gate (`--features pulsar`).
//!
//! Produces `n` rows into `events_json` (JSON payloads) and `events_avro`
//! (raw Avro datum payloads) over the broker's WebSocket producer API, and
//! prints the expected aggregate checksums the gate compares against.
//!
//!   cargo run --release --features pulsar --example pulsar_produce -- \
//!       http://127.0.0.1:8085 10000

#[cfg(not(feature = "pulsar"))]
fn main() {
    eprintln!("build with --features pulsar");
}

#[cfg(feature = "pulsar")]
fn main() {
    use base64::Engine as _;
    let args: Vec<String> = std::env::args().collect();
    let admin = args
        .get(1)
        .map(|s| s.as_str())
        .unwrap_or("http://127.0.0.1:8085");
    let n: u64 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(10_000);
    let ws_base = admin.replacen("http", "ws", 1);

    let avro_schema = apache_avro::Schema::parse_str(
        r#"{"type":"record","name":"Event","fields":[
            {"name":"id","type":"long"},
            {"name":"category","type":"string"},
            {"name":"value","type":"double"},
            {"name":"flag","type":"boolean"},
            {"name":"note","type":["null","string"]}
        ]}"#,
    )
    .expect("schema");

    let mut sum_value = 0.0f64;
    let mut flags = 0u64;
    for topic in ["events_json", "events_avro"] {
        let url = format!("{ws_base}/ws/v2/producer/persistent/public/default/{topic}");
        let (mut ws, _) = tungstenite::connect(&url).expect("producer ws");
        for i in 0..n {
            let value = (i % 1000) as f64 * 0.25;
            let category = ["red", "green", "blue"][(i % 3) as usize];
            let flag = i % 7 == 0;
            let note: Option<String> = (i % 5 == 0).then(|| format!("n{i}"));
            if topic == "events_json" {
                sum_value += value;
                flags += flag as u64;
            }
            let payload: Vec<u8> = if topic == "events_json" {
                serde_json::json!({
                    "id": i, "category": category, "value": value,
                    "flag": flag, "note": note,
                })
                .to_string()
                .into_bytes()
            } else {
                use apache_avro::types::Value as A;
                let rec = A::Record(vec![
                    ("id".into(), A::Long(i as i64)),
                    ("category".into(), A::String(category.into())),
                    ("value".into(), A::Double(value)),
                    ("flag".into(), A::Boolean(flag)),
                    (
                        "note".into(),
                        match &note {
                            Some(s) => A::Union(1, Box::new(A::String(s.clone()))),
                            None => A::Union(0, Box::new(A::Null)),
                        },
                    ),
                ]);
                apache_avro::to_avro_datum(&avro_schema, rec).expect("encode")
            };
            let msg = serde_json::json!({
                "payload": base64::engine::general_purpose::STANDARD.encode(&payload),
                "key": format!("k{}", i % 100),
            });
            ws.send(tungstenite::Message::Text(msg.to_string().into()))
                .expect("send");
            // Read the ack to keep the socket flowing.
            loop {
                match ws.read().expect("ack") {
                    tungstenite::Message::Text(t) => {
                        let v: serde_json::Value = serde_json::from_str(&t).unwrap();
                        assert_eq!(v["result"].as_str(), Some("ok"), "produce failed: {t}");
                        break;
                    }
                    _ => continue,
                }
            }
        }
        let _ = ws.close(None);
        eprintln!("produced {n} -> {topic}");
    }
    // The gate parses these.
    println!("EXPECT rows={n}");
    println!("EXPECT sum_value={sum_value:.2}");
    println!("EXPECT flags={flags}");
}
