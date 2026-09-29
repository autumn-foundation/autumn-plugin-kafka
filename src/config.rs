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

impl Default for KafkaConfig {
    fn default() -> Self {
        todo!()
    }
}

impl Default for ProducerSettings {
    fn default() -> Self {
        todo!()
    }
}

impl Default for HealthSettings {
    fn default() -> Self {
        todo!()
    }
}

impl std::fmt::Debug for KafkaConfig {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl KafkaConfig {
    /// Makes a config with default values and the given brokers.
    #[must_use]
    pub fn new(_brokers: impl Into<String>) -> Self {
        todo!()
    }

    /// Reads the `[kafka]` section of one TOML document.
    ///
    /// A missing section gives the default config.
    /// This function does not apply profiles or environment variables.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::Config`] if the TOML is not valid.
    pub fn from_toml_str(_toml: &str) -> Result<Self, KafkaError> {
        todo!()
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
    pub fn load_with_env(
        _profile: &str,
        _env: &HashMap<String, String>,
    ) -> Result<Self, KafkaError> {
        todo!()
    }

    /// Makes sure that the config can work.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::Config`] with the first problem found.
    pub fn validate(&self) -> Result<(), KafkaError> {
        todo!()
    }

    /// Returns the full `librdkafka` property map for one client role.
    ///
    /// Role properties override shared properties.
    /// Shared properties override `brokers` and `client_id`.
    #[must_use]
    pub fn client_properties(&self, _role: ClientRole) -> BTreeMap<String, String> {
        todo!()
    }
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
            ("client_id", KafkaConfig {
                client_id: String::new(),
                ..KafkaConfig::default()
            }),
            ("group_id", KafkaConfig {
                group_id: Some(" ".into()),
                ..KafkaConfig::default()
            }),
            ("send_timeout_ms", KafkaConfig {
                producer: ProducerSettings {
                    send_timeout_ms: 0,
                    ..ProducerSettings::default()
                },
                ..KafkaConfig::default()
            }),
            ("health.timeout_ms", KafkaConfig {
                health: HealthSettings {
                    timeout_ms: 0,
                    ..HealthSettings::default()
                },
                ..KafkaConfig::default()
            }),
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
        config.producer.properties.insert("y".into(), "producer".into());
        config.consumer.properties.insert("y".into(), "consumer".into());

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
