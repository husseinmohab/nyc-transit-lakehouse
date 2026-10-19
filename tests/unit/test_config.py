"""config tests

defaults that have to work and validation that has to bite
"""

from __future__ import annotations

import pytest

from nyc_transit.config import (
    FeedSettings,
    KafkaSettings,
    LakehouseSettings,
    ProducerSettings,
    Settings,
)


class TestKafkaSettings:
    def test_defaults_target_the_compose_broker(self):
        assert KafkaSettings().bootstrap_servers == "localhost:9092"

    def test_librdkafka_config_sets_the_durability_options(self):
        """acks=all plus idempotence is the Kafka half of exactly-once"""
        config = KafkaSettings().librdkafka_config()
        assert config["acks"] == "all"
        assert config["enable.idempotence"] is True

    def test_sasl_mechanism_is_omitted_when_unset(self):
        """librdkafka rejects an empty sasl.mechanism"""
        assert "sasl.mechanism" not in KafkaSettings().librdkafka_config()

    def test_sasl_mechanism_is_included_when_set(self):
        config = KafkaSettings(sasl_mechanism="SCRAM-SHA-512").librdkafka_config()
        assert config["sasl.mechanism"] == "SCRAM-SHA-512"

    def test_msk_iam_is_translated_to_oauthbearer(self):
        """librdkafka has no AWS_MSK_IAM mechanism

        the java client names it that and does the exchange over OAUTHBEARER
        passing AWS_MSK_IAM straight through makes librdkafka reject the config
        """
        config = KafkaSettings(sasl_mechanism="AWS_MSK_IAM").librdkafka_config()
        assert config["sasl.mechanism"] == "OAUTHBEARER"

    def test_msk_iam_forces_tls(self):
        """msk only listens for iam on tls so PLAINTEXT here is always wrong"""
        config = KafkaSettings(sasl_mechanism="AWS_MSK_IAM").librdkafka_config()
        assert config["security.protocol"] == "SASL_SSL"

    def test_uses_msk_iam_is_case_insensitive(self):
        assert KafkaSettings(sasl_mechanism="aws_msk_iam").uses_msk_iam
        assert not KafkaSettings().uses_msk_iam
        assert not KafkaSettings(sasl_mechanism="PLAIN").uses_msk_iam

    def test_topics_are_versioned(self):
        """A breaking schema change becomes a new topic not a poison pill"""
        kafka = KafkaSettings()
        for record_type in ("trip_update", "vehicle_position", "alert"):
            assert kafka.topic_for(record_type).endswith(".v1")

    def test_topic_lookup_is_exhaustive_over_record_types(self):
        from nyc_transit.schemas import RECORD_TYPES

        kafka = KafkaSettings()
        assert {kafka.topic_for(rt) for rt in RECORD_TYPES} == {
            kafka.topic_trip_updates,
            kafka.topic_vehicle_positions,
            kafka.topic_alerts,
        }

    def test_unknown_record_type_raises(self):
        with pytest.raises(ValueError, match="no topic configured"):
            KafkaSettings().topic_for("nope")

    def test_env_prefix_is_applied(self, monkeypatch):
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker:9094")
        assert KafkaSettings().bootstrap_servers == "broker:9094"


class TestFeedSettings:
    def test_rejects_an_abusive_poll_interval(self):
        """The MTA publishes every 15-30s polling faster is just rude"""
        with pytest.raises(ValueError, match="abusive"):
            FeedSettings(poll_interval_seconds=1.0)

    def test_accepts_the_documented_floor(self):
        assert FeedSettings(poll_interval_seconds=5.0).poll_interval_seconds == 5.0

    def test_enabled_parses_a_comma_list(self):
        assert FeedSettings(enabled_feeds="subway, alerts ,bus").enabled() == [
            "subway",
            "alerts",
            "bus",
        ]

    def test_bus_key_is_absent_by_default(self):
        assert FeedSettings().bus_api_key is None


class TestLakehouseSettings:
    def test_rejects_an_unknown_catalog_type(self):
        with pytest.raises(ValueError, match="unsupported catalog_type"):
            LakehouseSettings(catalog_type="hive")

    @pytest.mark.parametrize("catalog_type", ["hadoop", "glue", "rest"])
    def test_accepts_the_supported_catalog_types(self, catalog_type):
        assert LakehouseSettings(catalog_type=catalog_type).catalog_type == catalog_type

    def test_table_helpers_build_three_part_names(self):
        settings = LakehouseSettings(catalog_name="lh")
        assert settings.bronze("trip_updates") == "lh.bronze.trip_updates"
        assert settings.silver("vehicle_positions") == "lh.silver.vehicle_positions"

    def test_each_query_gets_its_own_checkpoint_directory(self):
        """Two queries sharing a checkpoint corrupt each other's offset log."""
        settings = LakehouseSettings(checkpoint_root="/tmp/cp")
        assert settings.checkpoint_for("a") != settings.checkpoint_for("b")
        assert settings.checkpoint_for("a") == "/tmp/cp/a"

    def test_checkpoint_root_trailing_slash_is_normalised(self):
        settings = LakehouseSettings(checkpoint_root="s3a://b/cp/")
        assert settings.checkpoint_for("q") == "s3a://b/cp/q"

    def test_iceberg_coordinate_is_scoped_to_the_spark_minor(self):
        """The coordinate embeds the Spark minor version, so
        requirements/spark.txt pins pyspark to the 3.5 line
        """
        coordinate = LakehouseSettings(
            spark_major_minor="3.5", scala_version="2.12", iceberg_version="1.7.1"
        ).iceberg_runtime_coordinate()
        assert coordinate == "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1"


class TestProducerSettings:
    def test_defaults_to_avro(self):
        assert ProducerSettings().serialization == "avro"

    def test_rejects_an_unknown_serialization(self):
        with pytest.raises(ValueError, match="must be 'avro' or 'json'"):
            ProducerSettings(serialization="protobuf")


class TestSettings:
    def test_an_empty_environment_yields_a_working_local_config(self):
        """The defaults are working defaults not placeholders"""
        settings = Settings()
        assert settings.kafka.bootstrap_servers
        assert settings.lakehouse.warehouse
        assert settings.feeds.poll_interval_seconds >= 5

    def test_nested_sections_are_independent_instances(self):
        a, b = Settings(), Settings()
        assert a.kafka is not b.kafka
