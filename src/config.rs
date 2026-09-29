//! The `[kafka]` config section.
//!
//! ```toml
//! [kafka]
//! brokers = "localhost:9092"
//! client_id = "orders-api"
//! group_id = "orders-api"
//!
//! [kafka.properties]            # all clients
//! "security.protocol" = "SASL_SSL"
//! "sasl.password" = "${KAFKA_PASSWORD}"
//!
//! [kafka.producer.properties]   # producer only
//! "linger.ms" = "5"
//!
//! [kafka.consumer.properties]   # consumers only
//! "auto.offset.reset" = "earliest"
//! ```

use std::collections::{BTreeMap, HashMap};

use serde::Deserialize;

use crate::error::KafkaError;

/// The kind of Kafka client that a property map is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientRole {
    /// A producer.
    Producer,
    /// A consumer.
    Consumer,
}

/// Settings for the plugin. Maps to the `[kafka]` section.
#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct KafkaConfig {
    /// Comma-separated `host:port` list. Default: `localhost:9092`.
    pub brokers: String,
    /// The Kafka `client.id`. Default: `autumn`.
    pub client_id: String,
    /// The default consumer group. A consumer can set its own group.
    pub group_id: Option<String>,
    /// `librdkafka` properties for all clients.
    pub properties: BTreeMap<String, String>,
    /// Producer settings.
    pub producer: ProducerSettings,
    /// Consumer settings.
    pub consumer: ConsumerSettings,
    /// Health indicator settings.
    pub health: HealthSettings,
    /// Time to wait for consumers and the producer at shutdown. Default: 10000.
    pub shutdown_timeout_ms: u64,
}

/// Producer settings. Maps to `[kafka.producer]`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ProducerSettings {
    /// `librdkafka` properties for the producer only.
    pub properties: BTreeMap<String, String>,
    /// Maximum time for one send, in milliseconds. Default: 5000.
    pub send_timeout_ms: u64,
}

/// Consumer settings. Maps to `[kafka.consumer]`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ConsumerSettings {
    /// `librdkafka` properties for consumers only.
    pub properties: BTreeMap<String, String>,
}

/// Health indicator settings. Maps to `[kafka.health]`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct HealthSettings {
    /// If `true`, a broker outage also fails `/ready`. Default: `false`.
    pub readiness: bool,
    /// Maximum time for the broker probe, in milliseconds. Default: 1500.
    pub timeout_ms: u64,
}

const REDACTED: &str = "<redacted>";

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            brokers: "localhost:9092".to_owned(),
            client_id: "autumn".to_owned(),
            group_id: None,
            properties: BTreeMap::new(),
            producer: ProducerSettings::default(),
            consumer: ConsumerSettings::default(),
            health: HealthSettings::default(),
            shutdown_timeout_ms: 10_000,
        }
    }
}

impl Default for ProducerSettings {
    fn default() -> Self {
        Self {
            properties: BTreeMap::new(),
            send_timeout_ms: 5000,
        }
    }
}

impl Default for HealthSettings {
    fn default() -> Self {
        Self {
            readiness: false,
            timeout_ms: 1500,
        }
    }
}

impl std::fmt::Debug for KafkaConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaConfig")
            .field("brokers", &self.brokers)
            .field("client_id", &self.client_id)
            .field("group_id", &self.group_id)
            .field("properties", &redacted(&self.properties))
            .field("producer.properties", &redacted(&self.producer.properties))
            .field("producer.send_timeout_ms", &self.producer.send_timeout_ms)
            .field("consumer.properties", &redacted(&self.consumer.properties))
            .field("health", &self.health)
            .field("shutdown_timeout_ms", &self.shutdown_timeout_ms)
            .finish()
    }
}

/// Returns `true` if the property value can hold a secret.
fn is_secret_key(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    ["password", "secret", "ssl.key.pem", "oauthbearer.config"]
        .iter()
        .any(|part| key.contains(part))
}

fn redacted(map: &BTreeMap<String, String>) -> BTreeMap<&str, &str> {
    map.iter()
        .map(|(k, v)| {
            let value = if is_secret_key(k) {
                REDACTED
            } else {
                v.as_str()
            };
            (k.as_str(), value)
        })
        .collect()
}

impl KafkaConfig {
    /// Makes a config with default values and the given brokers.
    #[must_use]
    pub fn new(brokers: impl Into<String>) -> Self {
        Self {
            brokers: brokers.into(),
            ..Self::default()
        }
    }

    /// Reads the `[kafka]` section of one TOML document.
    ///
    /// A missing section gives the default config.
    /// This function does not apply profiles or environment variables.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::Config`] if the TOML is not valid.
    pub fn from_toml_str(toml: &str) -> Result<Self, KafkaError> {
        let root = parse_table(toml, "TOML input")?;
        let mut section = toml::Table::new();
        merge_section(&mut section, root.get("kafka"), "[kafka]")?;
        from_table(section)
    }

    /// Loads the config with the same layers as Autumn.
    ///
    /// The layers are, from low to high priority:
    /// `autumn.toml`, `[profile.<profile>.kafka]`, `autumn-<profile>.toml`,
    /// and the `AUTUMN_KAFKA__*` environment variables.
    /// Then the function replaces each `${NAME}` with the variable `NAME`.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::Config`] if a file is not valid,
    /// or if a `${NAME}` variable is not set.
    pub fn load(profile: &str) -> Result<Self, KafkaError> {
        let env: HashMap<String, String> = std::env::vars().collect();
        Self::load_with_env(profile, &env)
    }

    /// Does the same as [`load`](Self::load), but reads variables from `env`.
    ///
    /// # Errors
    ///
    /// See [`load`](Self::load).
    pub fn load_with_env(profile: &str, env: &HashMap<String, String>) -> Result<Self, KafkaError> {
        let mut section = toml::Table::new();

        if let Some(base) = read_file("autumn.toml", env)? {
            merge_section(&mut section, base.get("kafka"), "[kafka]")?;
            let inline = base
                .get("profile")
                .and_then(|p| p.get(profile))
                .and_then(|p| p.get("kafka"));
            merge_section(&mut section, inline, "[profile.<name>.kafka]")?;
        }
        if let Some(file) = read_file(&format!("autumn-{profile}.toml"), env)? {
            merge_section(&mut section, file.get("kafka"), "[kafka]")?;
        }

        interpolate_table(&mut section, env)?;
        let mut config = from_table(section)?;

        if let Some(v) = env.get("AUTUMN_KAFKA__BROKERS") {
            config.brokers.clone_from(v);
        }
        if let Some(v) = env.get("AUTUMN_KAFKA__CLIENT_ID") {
            config.client_id.clone_from(v);
        }
        if let Some(v) = env.get("AUTUMN_KAFKA__GROUP_ID") {
            config.group_id = Some(v.clone());
        }
        Ok(config)
    }

    /// Makes sure that the config can work.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::Config`] with the first problem found.
    pub fn validate(&self) -> Result<(), KafkaError> {
        let fail = |msg: &str| Err(KafkaError::Config(msg.to_owned()));
        if self.brokers.trim().is_empty() {
            return fail("brokers must not be empty");
        }
        if self.client_id.trim().is_empty() {
            return fail("client_id must not be empty");
        }
        if self
            .group_id
            .as_deref()
            .is_some_and(|g| g.trim().is_empty())
        {
            return fail("group_id must not be blank");
        }
        if self.producer.send_timeout_ms == 0 {
            return fail("producer.send_timeout_ms must be greater than 0");
        }
        if self.health.timeout_ms == 0 {
            return fail("health.timeout_ms must be greater than 0");
        }
        Ok(())
    }

    /// Returns the full `librdkafka` property map for one client role.
    ///
    /// Role properties override shared properties.
    /// Shared properties override `brokers` and `client_id`.
    #[must_use]
    pub fn client_properties(&self, role: ClientRole) -> BTreeMap<String, String> {
        let mut props = BTreeMap::new();
        props.insert("bootstrap.servers".to_owned(), self.brokers.clone());
        props.insert("client.id".to_owned(), self.client_id.clone());
        props.extend(self.properties.clone());
        let role_props = match role {
            ClientRole::Producer => &self.producer.properties,
            ClientRole::Consumer => &self.consumer.properties,
        };
        props.extend(role_props.clone());
        props
    }
}

fn parse_table(text: &str, origin: &str) -> Result<toml::Table, KafkaError> {
    toml::from_str(text).map_err(|e| KafkaError::Config(format!("{origin}: {e}")))
}

fn from_table(section: toml::Table) -> Result<KafkaConfig, KafkaError> {
    toml::Value::Table(section)
        .try_into()
        .map_err(|e: toml::de::Error| KafkaError::Config(format!("[kafka]: {e}")))
}

/// Reads a config file with the same lookup as Autumn.
///
/// The lookup uses `AUTUMN_MANIFEST_DIR` first, then the current directory.
fn read_file(name: &str, env: &HashMap<String, String>) -> Result<Option<toml::Table>, KafkaError> {
    let path = env
        .get("AUTUMN_MANIFEST_DIR")
        .map(|dir| std::path::Path::new(dir).join(name))
        .filter(|p| p.exists())
        .unwrap_or_else(|| name.into());
    match std::fs::read_to_string(&path) {
        Ok(text) => parse_table(&text, &path.display().to_string()).map(Some),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(KafkaError::Config(format!("{}: {e}", path.display()))),
    }
}

fn merge_section(
    target: &mut toml::Table,
    layer: Option<&toml::Value>,
    origin: &str,
) -> Result<(), KafkaError> {
    match layer {
        None => Ok(()),
        Some(toml::Value::Table(t)) => {
            merge_tables(target, t);
            Ok(())
        }
        Some(_) => Err(KafkaError::Config(format!("{origin} must be a table"))),
    }
}

/// Merges `layer` into `target`. Nested tables merge. Other values replace.
fn merge_tables(target: &mut toml::Table, layer: &toml::Table) {
    for (key, value) in layer {
        match (target.get_mut(key), value) {
            (Some(toml::Value::Table(t)), toml::Value::Table(l)) => merge_tables(t, l),
            _ => {
                target.insert(key.clone(), value.clone());
            }
        }
    }
}

fn interpolate_table(
    table: &mut toml::Table,
    env: &HashMap<String, String>,
) -> Result<(), KafkaError> {
    for (_, value) in table.iter_mut() {
        match value {
            toml::Value::String(s) => *s = interpolate(s, env)?,
            toml::Value::Table(t) => interpolate_table(t, env)?,
            _ => {}
        }
    }
    Ok(())
}

/// Replaces each `${NAME}` in `text` with the variable `NAME`.
fn interpolate(text: &str, env: &HashMap<String, String>) -> Result<String, KafkaError> {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let end = after
            .find('}')
            .ok_or_else(|| KafkaError::Config(format!("unclosed \"${{\" in {text:?}")))?;
        let name = &after[..end];
        let value = env
            .get(name)
            .ok_or_else(|| KafkaError::Config(format!("environment variable {name} is not set")))?;
        out.push_str(value);
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    #[test]
    fn missing_section_gives_defaults() {
        let config = KafkaConfig::from_toml_str("[server]\nport = 3000\n").unwrap();
        assert_eq!(config, KafkaConfig::default());
        assert_eq!(config.brokers, "localhost:9092");
        assert_eq!(config.client_id, "autumn");
        assert_eq!(config.group_id, None);
        assert_eq!(config.producer.send_timeout_ms, 5000);
        assert!(!config.health.readiness);
        assert_eq!(config.health.timeout_ms, 1500);
        assert_eq!(config.shutdown_timeout_ms, 10_000);
    }

    #[test]
    fn new_sets_brokers() {
        let config = KafkaConfig::new("k1:9092");
        assert_eq!(config.brokers, "k1:9092");
        assert_eq!(config.client_id, "autumn");
    }

    #[test]
    fn parses_section_values() {
        let config = KafkaConfig::from_toml_str(
            r#"
            [kafka]
            brokers = "k1:9092,k2:9092"
            client_id = "api"
            group_id = "workers"
            shutdown_timeout_ms = 500

            [kafka.properties]
            "security.protocol" = "SSL"

            [kafka.producer]
            send_timeout_ms = 250
            properties = { "linger.ms" = "5" }

            [kafka.consumer.properties]
            "auto.offset.reset" = "earliest"

            [kafka.health]
            readiness = true
            timeout_ms = 300
            "#,
        )
        .unwrap();
        assert_eq!(config.brokers, "k1:9092,k2:9092");
        assert_eq!(config.client_id, "api");
        assert_eq!(config.group_id.as_deref(), Some("workers"));
        assert_eq!(config.shutdown_timeout_ms, 500);
        assert_eq!(config.properties["security.protocol"], "SSL");
        assert_eq!(config.producer.send_timeout_ms, 250);
        assert_eq!(config.producer.properties["linger.ms"], "5");
        assert_eq!(config.consumer.properties["auto.offset.reset"], "earliest");
        assert!(config.health.readiness);
        assert_eq!(config.health.timeout_ms, 300);
    }

    #[test]
    fn unknown_field_is_an_error() {
        let err = KafkaConfig::from_toml_str("[kafka]\nbroker = \"x:1\"\n").unwrap_err();
        assert!(matches!(err, KafkaError::Config(_)), "{err}");
        assert!(err.to_string().contains("broker"), "{err}");
    }

    #[test]
    fn load_applies_layers_in_order() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("autumn.toml"),
            r#"
            [kafka]
            brokers = "base:9092"
            client_id = "base"
            group_id = "base"
            [kafka.properties]
            "a" = "base"
            "b" = "base"

            [profile.prod.kafka]
            client_id = "inline"
            [profile.prod.kafka.properties]
            "a" = "inline"
            "#,
        )
        .unwrap();
        std::fs::write(
            dir.path().join("autumn-prod.toml"),
            "[kafka]\ngroup_id = \"file\"\n",
        )
        .unwrap();
        let vars = env(&[
            ("AUTUMN_MANIFEST_DIR", dir.path().to_str().unwrap()),
            ("AUTUMN_KAFKA__BROKERS", "env:9092"),
        ]);

        let config = KafkaConfig::load_with_env("prod", &vars).unwrap();

        assert_eq!(config.brokers, "env:9092");
        assert_eq!(config.client_id, "inline");
        assert_eq!(config.group_id.as_deref(), Some("file"));
        assert_eq!(config.properties["a"], "inline");
        assert_eq!(config.properties["b"], "base");
    }

    #[test]
    fn load_ignores_other_profiles() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("autumn.toml"),
            "[kafka]\nclient_id = \"base\"\n[profile.prod.kafka]\nclient_id = \"prod\"\n",
        )
        .unwrap();
        let vars = env(&[("AUTUMN_MANIFEST_DIR", dir.path().to_str().unwrap())]);

        let config = KafkaConfig::load_with_env("dev", &vars).unwrap();

        assert_eq!(config.client_id, "base");
    }

    #[test]
    fn load_applies_all_env_overrides() {
        let dir = tempfile::tempdir().unwrap();
        let vars = env(&[
            ("AUTUMN_MANIFEST_DIR", dir.path().to_str().unwrap()),
            ("AUTUMN_KAFKA__BROKERS", "e:1"),
            ("AUTUMN_KAFKA__CLIENT_ID", "e-client"),
            ("AUTUMN_KAFKA__GROUP_ID", "e-group"),
        ]);

        let config = KafkaConfig::load_with_env("dev", &vars).unwrap();

        assert_eq!(config.brokers, "e:1");
        assert_eq!(config.client_id, "e-client");
        assert_eq!(config.group_id.as_deref(), Some("e-group"));
    }

    #[test]
    fn load_replaces_env_placeholders() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("autumn.toml"),
            r#"
            [kafka]
            brokers = "${KAFKA_HOST}:9092"
            [kafka.properties]
            "sasl.password" = "${KAFKA_PASSWORD}"
            "#,
        )
        .unwrap();
        let vars = env(&[
            ("AUTUMN_MANIFEST_DIR", dir.path().to_str().unwrap()),
            ("KAFKA_HOST", "k"),
            ("KAFKA_PASSWORD", "s3cret"),
        ]);

        let config = KafkaConfig::load_with_env("dev", &vars).unwrap();

        assert_eq!(config.brokers, "k:9092");
        assert_eq!(config.properties["sasl.password"], "s3cret");
    }

    #[test]
    fn load_fails_when_placeholder_is_not_set() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("autumn.toml"),
            "[kafka.properties]\n\"sasl.password\" = \"${NOT_SET_ANYWHERE}\"\n",
        )
        .unwrap();
        let vars = env(&[("AUTUMN_MANIFEST_DIR", dir.path().to_str().unwrap())]);

        let err = KafkaConfig::load_with_env("dev", &vars).unwrap_err();

        assert!(err.to_string().contains("NOT_SET_ANYWHERE"), "{err}");
    }

    #[test]
    fn load_with_no_files_gives_defaults() {
        let dir = tempfile::tempdir().unwrap();
        let vars = env(&[("AUTUMN_MANIFEST_DIR", dir.path().to_str().unwrap())]);

        let config = KafkaConfig::load_with_env("dev", &vars).unwrap();

        assert_eq!(config, KafkaConfig::default());
    }

    #[test]
    fn validate_accepts_defaults() {
        KafkaConfig::default().validate().unwrap();
    }

    #[test]
    fn validate_rejects_bad_values() {
        let cases: Vec<(&str, KafkaConfig)> = vec![
            ("brokers", KafkaConfig::new("  ")),
            (
                "client_id",
                KafkaConfig {
                    client_id: String::new(),
                    ..KafkaConfig::default()
                },
            ),
            (
                "group_id",
                KafkaConfig {
                    group_id: Some(" ".into()),
                    ..KafkaConfig::default()
                },
            ),
            (
                "send_timeout_ms",
                KafkaConfig {
                    producer: ProducerSettings {
                        send_timeout_ms: 0,
                        ..ProducerSettings::default()
                    },
                    ..KafkaConfig::default()
                },
            ),
            (
                "health.timeout_ms",
                KafkaConfig {
                    health: HealthSettings {
                        timeout_ms: 0,
                        ..HealthSettings::default()
                    },
                    ..KafkaConfig::default()
                },
            ),
        ];
        for (field, config) in cases {
            let err = config.validate().unwrap_err();
            assert!(err.to_string().contains(field), "{field}: {err}");
        }
    }

    #[test]
    fn debug_redacts_secret_properties() {
        let mut config = KafkaConfig::default();
        config
            .properties
            .insert("sasl.password".into(), "hunter2".into());
        config
            .producer
            .properties
            .insert("ssl.key.password".into(), "hunter3".into());
        config
            .consumer
            .properties
            .insert("sasl.oauthbearer.client.secret".into(), "hunter4".into());
        config
            .properties
            .insert("sasl.username".into(), "alice".into());

        let text = format!("{config:?}");

        assert!(!text.contains("hunter"), "{text}");
        assert!(text.contains("alice"), "{text}");
        assert!(text.contains("<redacted>"), "{text}");
    }

    #[test]
    fn client_properties_merge_in_order() {
        let mut config = KafkaConfig::new("k:1");
        config.client_id = "api".into();
        config.properties.insert("x".into(), "shared".into());
        config.properties.insert("y".into(), "shared".into());
        config
            .producer
            .properties
            .insert("y".into(), "producer".into());
        config
            .consumer
            .properties
            .insert("y".into(), "consumer".into());

        let producer = config.client_properties(ClientRole::Producer);
        let consumer = config.client_properties(ClientRole::Consumer);

        assert_eq!(producer["bootstrap.servers"], "k:1");
        assert_eq!(producer["client.id"], "api");
        assert_eq!(producer["x"], "shared");
        assert_eq!(producer["y"], "producer");
        assert_eq!(consumer["y"], "consumer");
    }

    #[test]
    fn shared_properties_override_brokers() {
        let mut config = KafkaConfig::new("k:1");
        config
            .properties
            .insert("bootstrap.servers".into(), "other:2".into());

        let props = config.client_properties(ClientRole::Producer);

        assert_eq!(props["bootstrap.servers"], "other:2");
    }
}
