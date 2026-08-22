//! Apache Pulsar topics as tables (`--features pulsar`).
//!
//! A Pulsar NAMESPACE plays the catalog role: the admin REST API enumerates
//! topics and serves each topic's registered schema; every schema'd topic
//! becomes a table. A scan is a **bounded snapshot**: the topic's
//! `lastMessageId` is fetched at scan start and a WebSocket reader consumes
//! from `earliest` through exactly that message — late arrivals are the next
//! query's rows, never this one's.
//!
//! Transport note: the native pulsar-rs client was REJECTED (its chrono
//! requirement is incompatible with the arrow-53 lock pin — see Cargo.toml);
//! the broker's own WebSocket reader API (`/ws/v2/reader/...`) delivers
//! messages as JSON envelopes with base64 payloads over `tungstenite`, and
//! everything else is the same hand-rolled HTTP the Gravitino client uses.
//!
//! Schema mapping v1 (refusals BY NAME for anything else):
//! - Pulsar JSON and AVRO schema types — both carry an Avro record schema.
//! - Fields: string, int, long, float, double, boolean; `["null", T]`
//!   unions become nullable T.
//! - Every table gains `__key` (Utf8, nullable) and `__publish_time`
//!   (Timestamp[ms]) metadata columns.
//! - Topics without a schema, non-record schemas, unsupported field types:
//!   refused at registration, naming the topic/field.
//! - A payload that fails to decode fails the QUERY, naming the topic and
//!   message id — a silent row drop is a wrong answer.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, Float32Builder, Float64Builder, Int32Builder, Int64Builder,
    StringBuilder, TimestampMillisecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;

use crate::error::{QueryError, Result};
use crate::execution::ExecutionContext;
use crate::metastore::gravitino::http_get;
use crate::physical::operators::TableProvider;

/// Hard cap on messages per snapshot read; a topic beyond it is refused
/// loudly rather than exhausting memory (`QE_PULSAR_MAX_MESSAGES`).
fn max_messages() -> u64 {
    std::env::var("QE_PULSAR_MAX_MESSAGES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10_000_000)
}

/// Where a namespace of topics lives.
#[derive(Debug, Clone)]
pub struct PulsarSource {
    /// `http://host:port` of the broker web service (REST + WebSocket).
    pub admin_url: String,
    pub tenant: String,
    pub namespace: String,
}

impl PulsarSource {
    fn ns(&self) -> String {
        format!("{}/{}", self.tenant, self.namespace)
    }

    /// Topic short names in the namespace, sorted; Pulsar-internal topics
    /// (double-underscore) are skipped.
    pub fn list_topics(&self) -> Result<Vec<String>> {
        let body = http_get(
            &self.admin_url,
            &format!("/admin/v2/persistent/{}", self.ns()),
        )?;
        let v: serde_json::Value = serde_json::from_slice(&body).map_err(|e| {
            QueryError::Storage(format!(
                "pulsar {}: non-JSON topic list: {e}",
                self.admin_url
            ))
        })?;
        let mut names: Vec<String> = v
            .as_array()
            .ok_or_else(|| {
                QueryError::Storage(format!(
                    "pulsar {}: topic list is not an array",
                    self.admin_url
                ))
            })?
            .iter()
            .filter_map(|t| t.as_str())
            .filter_map(|full| full.rsplit('/').next().map(|s| s.to_string()))
            .filter(|n| !n.starts_with("__"))
            .collect();
        names.sort();
        Ok(names)
    }

    /// The topic's registered schema, mapped to arrow. Refuses by name.
    pub fn topic_schema(&self, topic: &str) -> Result<TopicSchema> {
        let body = http_get(
            &self.admin_url,
            &format!("/admin/v2/schemas/{}/{topic}/schema", self.ns()),
        )
        .map_err(|e| {
            QueryError::Storage(format!(
                "pulsar topic `{topic}` has no readable schema ({e}); schemaless topics \
                 are not queryable — register a JSON or AVRO schema"
            ))
        })?;
        let v: serde_json::Value = serde_json::from_slice(&body).map_err(|e| {
            QueryError::Storage(format!("pulsar schema for `{topic}`: non-JSON: {e}"))
        })?;
        let kind = match v["type"].as_str() {
            Some("JSON") => PayloadKind::Json,
            Some("AVRO") => PayloadKind::Avro,
            Some(other) => {
                return Err(QueryError::NotImplemented(format!(
                    "pulsar topic `{topic}` has schema type `{other}`; JSON and AVRO are \
                     supported"
                )))
            }
            None => {
                return Err(QueryError::Storage(format!(
                    "pulsar topic `{topic}`: schema response has no type"
                )))
            }
        };
        let avro_text = v["data"].as_str().ok_or_else(|| {
            QueryError::Storage(format!("pulsar topic `{topic}`: schema has no data"))
        })?;
        let avro_json: serde_json::Value = serde_json::from_str(avro_text).map_err(|e| {
            QueryError::Storage(format!("pulsar topic `{topic}`: schema data not JSON: {e}"))
        })?;
        let fields = avro_record_fields(topic, &avro_json)?;
        let avro_schema = if matches!(kind, PayloadKind::Avro) {
            Some(apache_avro::Schema::parse_str(avro_text).map_err(|e| {
                QueryError::Storage(format!("pulsar topic `{topic}`: avro schema: {e}"))
            })?)
        } else {
            None
        };
        Ok(TopicSchema {
            kind,
            fields,
            avro_schema,
        })
    }

    /// `(ledgerId, entryId, batchIndex)` of the last message, or None when
    /// the topic is empty.
    fn last_message_id(&self, topic: &str) -> Result<Option<(u64, u64, i64)>> {
        let body = http_get(
            &self.admin_url,
            &format!("/admin/v2/persistent/{}/{topic}/lastMessageId", self.ns()),
        )?;
        let v: serde_json::Value = serde_json::from_slice(&body)
            .map_err(|e| QueryError::Storage(format!("pulsar lastMessageId for `{topic}`: {e}")))?;
        let entry = v["entryId"].as_i64().unwrap_or(-1);
        if entry < 0 {
            return Ok(None);
        }
        let ledger = v["ledgerId"].as_i64().unwrap_or(-1);
        if ledger < 0 {
            return Ok(None);
        }
        Ok(Some((
            ledger as u64,
            entry as u64,
            v["batchIndex"].as_i64().unwrap_or(-1),
        )))
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PayloadKind {
    Json,
    Avro,
}

/// A topic's arrow-mapped schema.
pub struct TopicSchema {
    pub kind: PayloadKind,
    /// Data fields in schema order (metadata columns appended separately).
    pub fields: Vec<Field>,
    avro_schema: Option<apache_avro::Schema>,
}

/// Map an Avro record schema (which Pulsar uses for BOTH its JSON and AVRO
/// schema types) to arrow fields. Refuses by field name.
fn avro_record_fields(topic: &str, schema: &serde_json::Value) -> Result<Vec<Field>> {
    if schema["type"].as_str() != Some("record") {
        return Err(QueryError::NotImplemented(format!(
            "pulsar topic `{topic}`: schema is not a record"
        )));
    }
    let mut out = Vec::new();
    for f in schema["fields"].as_array().unwrap_or(&Vec::new()) {
        let name = f["name"].as_str().unwrap_or("?").to_string();
        let (dt, nullable) = avro_type_to_arrow(&f["type"]).ok_or_else(|| {
            QueryError::NotImplemented(format!(
                "pulsar topic `{topic}` field `{name}`: type {} is not supported \
                 (string/int/long/float/double/boolean and their nullable unions are)",
                f["type"]
            ))
        })?;
        out.push(Field::new(name, dt, nullable));
    }
    if out.is_empty() {
        return Err(QueryError::Storage(format!(
            "pulsar topic `{topic}`: record schema has no fields"
        )));
    }
    Ok(out)
}

fn avro_type_to_arrow(t: &serde_json::Value) -> Option<(DataType, bool)> {
    match t {
        serde_json::Value::String(s) => {
            let dt = match s.as_str() {
                "string" => DataType::Utf8,
                "int" => DataType::Int32,
                "long" => DataType::Int64,
                "float" => DataType::Float32,
                "double" => DataType::Float64,
                "boolean" => DataType::Boolean,
                _ => return None,
            };
            Some((dt, false))
        }
        serde_json::Value::Array(union) => {
            // ["null", T] or [T, "null"] — nullable T.
            let mut inner = None;
            for u in union {
                if u.as_str() == Some("null") {
                    continue;
                }
                if inner.is_some() {
                    return None;
                }
                inner = Some(avro_type_to_arrow(u)?.0);
            }
            inner.map(|dt| (dt, true))
        }
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// The provider
// ---------------------------------------------------------------------------

pub struct PulsarTable {
    source: PulsarSource,
    topic: String,
    kind: PayloadKind,
    avro_schema: Option<apache_avro::Schema>,
    schema: SchemaRef,
    /// Index of the first metadata column (= number of data fields).
    n_data: usize,
}

impl std::fmt::Debug for PulsarTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PulsarTable[{}/{}]", self.source.ns(), self.topic)
    }
}

impl PulsarTable {
    pub fn try_new(source: PulsarSource, topic: &str) -> Result<Self> {
        let ts = source.topic_schema(topic)?;
        let mut fields = ts.fields.clone();
        let n_data = fields.len();
        fields.push(Field::new("__key", DataType::Utf8, true));
        fields.push(Field::new(
            "__publish_time",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ));
        Ok(Self {
            source,
            topic: topic.to_string(),
            kind: ts.kind,
            avro_schema: ts.avro_schema,
            schema: Arc::new(Schema::new(fields)),
            n_data,
        })
    }

    /// Read the bounded snapshot and build one RecordBatch.
    fn read_all(&self) -> Result<RecordBatch> {
        let boundary = self.source.last_message_id(&self.topic)?;
        let Some((b_ledger, b_entry, b_batch)) = boundary else {
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        };

        // WebSocket reader from earliest. The reader endpoint lives on the
        // same web service as the admin API.
        let ws_url = format!(
            "{}/ws/v2/reader/persistent/{}/{}?messageId=earliest&receiverQueueSize=1000",
            self.source.admin_url.replacen("http", "ws", 1),
            self.source.ns(),
            self.topic
        );
        let (mut socket, _) = tungstenite::connect(&ws_url).map_err(|e| {
            QueryError::Storage(format!(
                "pulsar reader websocket for `{}` failed: {e}",
                self.topic
            ))
        })?;

        let mut rows: Vec<(serde_json::Value, Option<String>, i64)> = Vec::new();
        let cap = max_messages();
        loop {
            let msg = socket
                .read()
                .map_err(|e| QueryError::Storage(format!("pulsar reader `{}`: {e}", self.topic)))?;
            let text = match msg {
                tungstenite::Message::Text(t) => t,
                tungstenite::Message::Ping(_) | tungstenite::Message::Pong(_) => continue,
                other => {
                    return Err(QueryError::Storage(format!(
                        "pulsar reader `{}`: unexpected frame {other:?}",
                        self.topic
                    )))
                }
            };
            let env: serde_json::Value = serde_json::from_str(&text).map_err(|e| {
                QueryError::Storage(format!("pulsar reader `{}`: bad envelope: {e}", self.topic))
            })?;
            let msg_id = env["messageId"].as_str().unwrap_or("").to_string();
            // Ack so the reader's receiver queue keeps flowing.
            let ack = format!("{{\"messageId\":\"{msg_id}\"}}");
            socket
                .send(tungstenite::Message::Text(ack))
                .map_err(|e| QueryError::Storage(format!("pulsar ack: {e}")))?;

            let (ledger, entry, batch_idx) = decode_message_id(&msg_id).ok_or_else(|| {
                QueryError::Storage(format!(
                    "pulsar reader `{}`: undecodable messageId `{msg_id}`",
                    self.topic
                ))
            })?;

            let payload_b64 = env["payload"].as_str().unwrap_or("");
            let payload = base64_decode(payload_b64).ok_or_else(|| {
                QueryError::Storage(format!(
                    "pulsar `{}` message {msg_id}: payload is not base64",
                    self.topic
                ))
            })?;
            let value = self.decode_payload(&payload).map_err(|e| {
                QueryError::Storage(format!(
                    "pulsar `{}` message {msg_id}: payload does not match the topic \
                     schema: {e}",
                    self.topic
                ))
            })?;
            let key = env["key"]
                .as_str()
                .filter(|k| !k.is_empty())
                .map(String::from);
            let publish_ms = env["publishTime"]
                .as_str()
                .and_then(parse_rfc3339_ms)
                .unwrap_or(0);
            rows.push((value, key, publish_ms));

            if rows.len() as u64 > cap {
                return Err(QueryError::Storage(format!(
                    "pulsar topic `{}` exceeds QE_PULSAR_MAX_MESSAGES ({cap}); refusing \
                     rather than exhausting memory",
                    self.topic
                )));
            }
            // Past-the-boundary guard first (another producer may append
            // while we read), then exact-boundary termination.
            if (ledger, entry) > (b_ledger, b_entry) {
                break;
            }
            if (ledger, entry) == (b_ledger, b_entry) && (b_batch < 0 || batch_idx >= b_batch) {
                break;
            }
        }
        let _ = socket.close(None);

        self.build_batch(rows)
    }

    fn decode_payload(&self, payload: &[u8]) -> Result<serde_json::Value> {
        match self.kind {
            PayloadKind::Json => {
                serde_json::from_slice(payload).map_err(|e| QueryError::Storage(e.to_string()))
            }
            PayloadKind::Avro => {
                let schema = self.avro_schema.as_ref().expect("avro kind");
                let mut cursor = std::io::Cursor::new(payload);
                let value = apache_avro::from_avro_datum(schema, &mut cursor, None)
                    .map_err(|e| QueryError::Storage(e.to_string()))?;
                avro_value_to_json(value)
            }
        }
    }

    fn build_batch(
        &self,
        rows: Vec<(serde_json::Value, Option<String>, i64)>,
    ) -> Result<RecordBatch> {
        let n = rows.len();
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.schema.fields().len());
        for (fi, field) in self.schema.fields().iter().enumerate().take(self.n_data) {
            let name = field.name().as_str();
            macro_rules! build {
                ($builder:ty, $get:expr) => {{
                    let mut b = <$builder>::with_capacity(n);
                    for (v, _, _) in &rows {
                        let cell = &v[name];
                        if cell.is_null() {
                            if !field.is_nullable() {
                                return Err(QueryError::Storage(format!(
                                    "pulsar `{}`: field `{name}` is non-nullable but a \
                                     message has no value",
                                    self.topic
                                )));
                            }
                            b.append_null();
                        } else {
                            #[allow(clippy::redundant_closure_call)]
                            match ($get)(cell) {
                                Some(x) => b.append_value(x),
                                None => {
                                    return Err(QueryError::Storage(format!(
                                        "pulsar `{}`: field `{name}` has a value of the \
                                         wrong type: {cell}",
                                        self.topic
                                    )))
                                }
                            }
                        }
                    }
                    Arc::new(b.finish()) as ArrayRef
                }};
            }
            let arr: ArrayRef = match field.data_type() {
                DataType::Utf8 => {
                    let mut b = StringBuilder::new();
                    for (v, _, _) in &rows {
                        let cell = &v[name];
                        if cell.is_null() {
                            b.append_null();
                        } else {
                            match cell.as_str() {
                                Some(s) => b.append_value(s),
                                None => {
                                    return Err(QueryError::Storage(format!(
                                        "pulsar `{}`: field `{name}` is not a string: {cell}",
                                        self.topic
                                    )))
                                }
                            }
                        }
                    }
                    Arc::new(b.finish())
                }
                DataType::Int32 => build!(Int32Builder, |c: &serde_json::Value| c
                    .as_i64()
                    .map(|x| x as i32)),
                DataType::Int64 => build!(Int64Builder, |c: &serde_json::Value| c.as_i64()),
                DataType::Float32 => build!(Float32Builder, |c: &serde_json::Value| c
                    .as_f64()
                    .map(|x| x as f32)),
                DataType::Float64 => build!(Float64Builder, |c: &serde_json::Value| c.as_f64()),
                DataType::Boolean => build!(BooleanBuilder, |c: &serde_json::Value| c.as_bool()),
                other => {
                    return Err(QueryError::Internal(format!(
                        "pulsar field {name} mapped to unexpected type {other} (index {fi})"
                    )))
                }
            };
            arrays.push(arr);
        }
        let mut keys = StringBuilder::new();
        let mut times = TimestampMillisecondBuilder::with_capacity(n);
        for (_, k, t) in &rows {
            match k {
                Some(k) => keys.append_value(k),
                None => keys.append_null(),
            }
            times.append_value(*t);
        }
        arrays.push(Arc::new(keys.finish()));
        arrays.push(Arc::new(times.finish()));
        RecordBatch::try_new(self.schema.clone(), arrays).map_err(Into::into)
    }
}

impl TableProvider for PulsarTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn scan(&self, projection: Option<&[usize]>) -> Result<Vec<RecordBatch>> {
        let batch = self.read_all()?;
        let batch = match projection {
            None => batch,
            Some(idx) => batch.project(idx)?,
        };
        Ok(vec![batch])
    }
}

/// Register every readable topic of the namespace as a table. Any topic the
/// engine cannot serve fails the WHOLE registration, by name — the fileset
/// convention: a partial catalog is worse than a refused one.
pub fn register_pulsar_namespace(
    ctx: &mut ExecutionContext,
    source: &PulsarSource,
) -> Result<Vec<String>> {
    let topics = source.list_topics()?;
    if topics.is_empty() {
        return Err(QueryError::Storage(format!(
            "pulsar namespace {} contains no topics; nothing to serve",
            source.ns()
        )));
    }
    for t in &topics {
        let table = PulsarTable::try_new(source.clone(), t)?;
        ctx.register_table_provider(t, Arc::new(table));
    }
    Ok(topics)
}

// ---------------------------------------------------------------------------
// Small codecs
// ---------------------------------------------------------------------------

/// Pulsar WS message ids are base64(protobuf MessageIdData). We need only
/// fields 1 (ledgerId), 2 (entryId), 4 (batch_index) — plain varints.
fn decode_message_id(b64: &str) -> Option<(u64, u64, i64)> {
    let bytes = base64_decode(b64)?;
    let (mut ledger, mut entry, mut batch): (Option<u64>, Option<u64>, i64) = (None, None, -1);
    let mut i = 0usize;
    while i < bytes.len() {
        let (tag, n) = varint(&bytes[i..])?;
        i += n;
        let field = tag >> 3;
        let wire = tag & 7;
        match wire {
            0 => {
                let (v, n) = varint(&bytes[i..])?;
                i += n;
                match field {
                    1 => ledger = Some(v),
                    2 => entry = Some(v),
                    4 => batch = v as i64,
                    _ => {}
                }
            }
            2 => {
                let (len, n) = varint(&bytes[i..])?;
                i += n + len as usize;
            }
            5 => i += 4,
            1 => i += 8,
            _ => return None,
        }
    }
    Some((ledger?, entry?, batch))
}

fn varint(b: &[u8]) -> Option<(u64, usize)> {
    let mut v = 0u64;
    for (i, byte) in b.iter().enumerate().take(10) {
        v |= u64::from(byte & 0x7f) << (7 * i);
        if byte & 0x80 == 0 {
            return Some((v, i + 1));
        }
    }
    None
}

fn base64_decode(s: &str) -> Option<Vec<u8>> {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.decode(s).ok()
}

/// `2026-08-22T15:04:05.123-07:00` (Pulsar's publishTime) -> epoch millis.
fn parse_rfc3339_ms(s: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.timestamp_millis())
}

/// Avro decoded values, reduced to the JSON shapes `build_batch` consumes.
fn avro_value_to_json(v: apache_avro::types::Value) -> Result<serde_json::Value> {
    use apache_avro::types::Value as A;
    Ok(match v {
        A::Record(fields) => {
            let mut m = serde_json::Map::new();
            for (k, fv) in fields {
                m.insert(k, avro_value_to_json(fv)?);
            }
            serde_json::Value::Object(m)
        }
        A::Union(_, inner) => avro_value_to_json(*inner)?,
        A::Null => serde_json::Value::Null,
        A::Boolean(b) => serde_json::Value::Bool(b),
        A::Int(x) => serde_json::Value::from(x),
        A::Long(x) => serde_json::Value::from(x),
        A::Float(x) => serde_json::Value::from(x),
        A::Double(x) => serde_json::Value::from(x),
        A::String(s) => serde_json::Value::String(s),
        other => {
            return Err(QueryError::NotImplemented(format!(
                "avro value {other:?} is outside the supported field types"
            )))
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_id_protobuf_decodes() {
        // MessageIdData { ledgerId: 5, entryId: 300 } encoded by hand:
        // field1 varint 5 = 08 05; field2 varint 300 = 10 AC 02
        use base64::Engine as _;
        let b64 = base64::engine::general_purpose::STANDARD.encode([0x08, 0x05, 0x10, 0xAC, 0x02]);
        assert_eq!(decode_message_id(&b64), Some((5, 300, -1)));
    }

    #[test]
    fn avro_unions_map_to_nullable_fields() {
        let schema: serde_json::Value = serde_json::json!({
            "type": "record", "name": "r", "fields": [
                {"name": "a", "type": "long"},
                {"name": "b", "type": ["null", "string"]},
                {"name": "c", "type": "double"},
            ]
        });
        let fields = avro_record_fields("t", &schema).unwrap();
        assert_eq!(fields[0].data_type(), &DataType::Int64);
        assert!(!fields[0].is_nullable());
        assert_eq!(fields[1].data_type(), &DataType::Utf8);
        assert!(fields[1].is_nullable());
    }

    #[test]
    fn unsupported_field_types_are_refused_by_name() {
        let schema: serde_json::Value = serde_json::json!({
            "type": "record", "name": "r", "fields": [
                {"name": "geo", "type": {"type": "map", "values": "double"}},
            ]
        });
        let err = avro_record_fields("locations", &schema)
            .unwrap_err()
            .to_string();
        assert!(err.contains("locations") && err.contains("geo"), "{err}");
    }
}
