"""msk sasl iam auth for the librdkafka producer

msk with SASL/IAM does not speak a sasl mechanism librdkafka knows about
the wire protocol is OAUTHBEARER and the token is a presigned sts url that the
aws signer builds from whatever credentials the default chain finds

so the producer needs a callback librdkafka invokes whenever the token is due
tokens last about fifteen minutes and librdkafka refreshes on its own as long
as the callback is registered

nothing here reads an access key the chain resolves the instance profile or
task role which is why modules/iam has no iam user in it
"""

from __future__ import annotations

from typing import Any

from nyc_transit.config import KafkaSettings


class MSKTokenError(RuntimeError):
    """raised when a token cannot be minted

    separate from a connection error because the fix is different one is
    credentials the other is the broker
    """


def _signer() -> Any:
    try:
        from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
    except ImportError as exc:  # pragma: no cover - depends on the install
        raise MSKTokenError(
            "KAFKA_SASL_MECHANISM=AWS_MSK_IAM needs aws-msk-iam-sasl-signer-python "
            "which is in requirements/base.txt"
        ) from exc
    return MSKAuthTokenProvider


def oauth_callback(region: str) -> Any:
    """build the oauth_cb librdkafka expects

    the callback gets handed the sasl.oauthbearer.config string which we do not
    use and must return the token with the moment it expires
    """
    provider = _signer()

    def callback(_config: str) -> tuple[str, float]:
        try:
            token, expiry_ms = provider.generate_auth_token(region)
        except Exception as exc:  # noqa: BLE001 - botocore raises many types
            raise MSKTokenError(f"could not mint an msk auth token in {region}: {exc}") from exc
        # librdkafka wants seconds since the epoch the signer hands back ms
        return token, expiry_ms / 1000.0

    return callback


def with_oauth_callback(config: dict[str, object], settings: KafkaSettings) -> dict[str, object]:
    """add the msk token callback when the mechanism calls for one

    a no op for PLAINTEXT so the local docker compose path is untouched
    """
    if not settings.uses_msk_iam:
        return config
    if not settings.aws_region:
        raise MSKTokenError("KAFKA_AWS_REGION must be set when KAFKA_SASL_MECHANISM=AWS_MSK_IAM")
    return {**config, "oauth_cb": oauth_callback(settings.aws_region)}
