"""msk sasl iam wiring for both clients

no broker and no network the token minting itself is the signer's job
what is tested here is that the right knobs are set for the right mechanism
and that the local PLAINTEXT path is not touched
"""

from __future__ import annotations

import pytest

from nyc_transit.config import KafkaSettings, LakehouseSettings, Settings
from nyc_transit.producer.msk_iam import MSKTokenError, with_oauth_callback
from nyc_transit.streaming.jobs import msk_iam_options
from nyc_transit.streaming.session import iceberg_packages


class TestProducerConfig:
    def test_plaintext_gets_no_callback(self):
        """the docker compose path must not need the signer installed"""
        kafka = KafkaSettings()
        config = with_oauth_callback(kafka.librdkafka_config(), kafka)
        assert "oauth_cb" not in config

    def test_msk_iam_gets_a_callback(self):
        kafka = KafkaSettings(sasl_mechanism="AWS_MSK_IAM", aws_region="us-east-1")
        config = with_oauth_callback(kafka.librdkafka_config(), kafka)
        assert callable(config["oauth_cb"])

    def test_msk_iam_without_a_region_fails_loudly(self):
        """the signer builds a presigned url so it cannot guess the region

        without this the failure surfaces as a connect timeout which sends you
        looking at security groups
        """
        kafka = KafkaSettings(sasl_mechanism="AWS_MSK_IAM")
        with pytest.raises(MSKTokenError, match="KAFKA_AWS_REGION"):
            with_oauth_callback(kafka.librdkafka_config(), kafka)

    def test_the_callback_does_not_mutate_the_input(self):
        kafka = KafkaSettings(sasl_mechanism="AWS_MSK_IAM", aws_region="us-east-1")
        base = kafka.librdkafka_config()
        with_oauth_callback(base, kafka)
        assert "oauth_cb" not in base


class TestSparkOptions:
    def test_plaintext_adds_nothing(self, monkeypatch):
        monkeypatch.delenv("KAFKA_SASL_MECHANISM", raising=False)
        assert msk_iam_options(Settings()) == {}

    def test_msk_iam_sets_the_login_module(self, monkeypatch):
        monkeypatch.setenv("KAFKA_SASL_MECHANISM", "AWS_MSK_IAM")
        monkeypatch.setenv("KAFKA_AWS_REGION", "us-east-1")
        options = msk_iam_options(Settings())
        assert options["kafka.sasl.mechanism"] == "AWS_MSK_IAM"
        assert "IAMLoginModule" in options["kafka.sasl.jaas.config"]
        assert "IAMClientCallbackHandler" in options["kafka.sasl.client.callback.handler.class"]

    def test_the_jar_is_only_added_for_msk_iam(self):
        """without the jar the driver dies on ClassNotFoundException

        with it on a plaintext cluster it is dead weight in every submit
        """
        lakehouse = LakehouseSettings()
        plain = iceberg_packages(lakehouse, with_kafka=True, with_s3=False)
        iam = iceberg_packages(lakehouse, with_kafka=True, with_s3=False, with_msk_iam=True)
        assert not any("aws-msk-iam-auth" in p for p in plain)
        assert any("aws-msk-iam-auth" in p for p in iam)

    def test_the_jar_needs_the_kafka_connector(self):
        """msk iam without the kafka source is a meaningless combination"""
        lakehouse = LakehouseSettings()
        packages = iceberg_packages(lakehouse, with_kafka=False, with_s3=False, with_msk_iam=True)
        assert not any("aws-msk-iam-auth" in p for p in packages)
