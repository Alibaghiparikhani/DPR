"""Local protocol exceptions. None of these represent user task failures."""


class ProtocolError(ValueError):
    """Base for bounded, deterministic protocol failures."""


class EncodingError(ProtocolError):
    """An object cannot be encoded as a supported message."""


class DecodingError(ProtocolError):
    """Bytes do not contain a valid control message."""


class ValidationError(EncodingError, DecodingError):
    """A known schema has invalid fields; applicable at either boundary."""


class ResourceLimitExceeded(ValidationError):
    """A structural, numeric, text, or collection budget was exceeded."""


class UnsupportedProtocolVersion(ValidationError):
    """No explicitly supported wire version matches the peer."""


class UnknownMessageType(DecodingError):
    """The stable wire type is not in the local allowlist."""


class RegistryError(ProtocolError):
    """A local schema registration conflicts with another entry."""


class FramingError(ProtocolError):
    """Invalid framing or use of a terminal decoder."""

    completed_frames: tuple[bytes, ...] = ()


class FrameTooLarge(FramingError):
    """The payload exceeds the configured (or absolute) limit."""


class TruncatedFrame(FramingError):
    """End of input occurred inside a header or payload."""


class DecoderStateError(FramingError):
    """A failed or closed decoder must be reset before reuse."""
