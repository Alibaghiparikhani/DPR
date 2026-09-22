"""Exact version selection; independent of package or Python versions."""

from .errors import UnsupportedProtocolVersion, ValidationError
from .limits import MAX_VERSION

PROTOCOL_VERSION = 1
SUPPORTED_VERSIONS = (PROTOCOL_VERSION,)


def validate_version(version: int) -> None:
    if type(version) is not int or not 1 <= version <= MAX_VERSION:
        raise ValidationError("protocol_version must be an integer in 1..65535")
    if version not in SUPPORTED_VERSIONS:
        raise UnsupportedProtocolVersion("unsupported protocol_version")


def negotiate_version(peer_versions: tuple[int, ...]) -> int:
    """Select the highest explicitly shared version; never guess compatibility."""
    if type(peer_versions) is not tuple or not 1 <= len(peer_versions) <= 32:
        raise ValidationError(
            "peer_versions must be a nonempty tuple of at most 32 versions"
        )
    if any(type(v) is not int or not 1 <= v <= MAX_VERSION for v in peer_versions):
        raise ValidationError("peer_versions contains an invalid version")
    if len(set(peer_versions)) != len(peer_versions):
        raise ValidationError("duplicate peer_versions")
    shared = set(peer_versions).intersection(SUPPORTED_VERSIONS)
    if not shared:
        raise UnsupportedProtocolVersion("no supported peer protocol version")
    return max(shared)
