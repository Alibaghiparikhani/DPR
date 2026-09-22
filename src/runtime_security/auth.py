"""Bounded HMAC node authentication for runtime transport handshakes.

This module performs no I/O. The live control transport has the verifier issue a
challenge, the peer prove it with its PSK, and then calls ``verify`` before
admitting worker or operator protocol traffic. Security state is bounded *without* evicting
unexpired challenges: when the configured window is full, challenge issuance
fails closed until entries expire.
"""
from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
import hashlib
import hmac
import secrets
import time
from typing import Callable, Mapping


class AuthenticationError(ValueError):
    pass


class ReplayDetected(AuthenticationError):
    pass


class AuthenticationCapacityExceeded(AuthenticationError):
    """Verifier cannot safely retain another unexpired challenge."""


@dataclass(frozen=True, slots=True)
class AuthChallenge:
    node_id: str
    session_id: str
    nonce: str
    issued_at: int


@dataclass(frozen=True, slots=True)
class AuthProof:
    node_id: str
    session_id: str
    nonce: str
    issued_at: int
    mac_hex: str


@dataclass(slots=True)
class _ChallengeState:
    issued_at: int
    consumed: bool = False


class NodeAuthenticator:
    """Per-node PSK challenge/response with bounded, fail-closed replay state.

    ``replay_limit`` is the maximum number of *unexpired issued challenges*
    retained by the verifier. Successful verification consumes a challenge but
    leaves its tombstone present until the acceptance window expires. Therefore
    capacity pressure can reject new handshakes, but can never make a still-valid
    proof replayable.
    """

    def __init__(
        self,
        node_secrets: Mapping[str, bytes],
        *,
        max_age_seconds: int = 60,
        replay_limit: int = 4096,
        per_node_challenge_limit: int | None = None,
        clock: Callable[[], float] = time.time,
        nonce_source: Callable[[], str] | None = None,
    ) -> None:
        if not node_secrets:
            raise ValueError("at least one node secret is required")
        normalized: dict[str, bytes] = {}
        for node_id, secret in node_secrets.items():
            if not isinstance(node_id, str) or not node_id.strip():
                raise ValueError("node ids must be nonempty text")
            if not isinstance(secret, bytes) or len(secret) < 32:
                raise ValueError("node secrets must contain at least 32 bytes")
            normalized[node_id] = bytes(secret)
        if type(max_age_seconds) is not int or max_age_seconds < 1:
            raise ValueError("max_age_seconds must be a positive integer")
        if type(replay_limit) is not int or replay_limit < 1:
            raise ValueError("replay_limit must be a positive integer")
        if per_node_challenge_limit is None:
            per_node_challenge_limit = min(8, replay_limit)
        if type(per_node_challenge_limit) is not int or per_node_challenge_limit < 1:
            raise ValueError("per_node_challenge_limit must be None or a positive integer")
        if per_node_challenge_limit > replay_limit:
            raise ValueError("per_node_challenge_limit cannot exceed replay_limit")
        self._secrets = normalized
        self.max_age_seconds = max_age_seconds
        self.replay_limit = replay_limit
        self.per_node_challenge_limit = per_node_challenge_limit
        self._clock = clock
        self._nonce_source = nonce_source or (lambda: secrets.token_hex(32))
        self._challenges: OrderedDict[tuple[str, str, str], _ChallengeState] = OrderedDict()

    @property
    def replay_cache_size(self) -> int:
        """Number of unexpired issued/consumed challenge records retained."""
        self._purge(int(self._clock()))
        return len(self._challenges)

    def issue(self, node_id: str, session_id: str) -> AuthChallenge:
        self._secret(node_id)
        self._require_text(session_id, "session_id")
        now = int(self._clock())
        self._purge(now)
        if len(self._challenges) >= self.replay_limit:
            raise AuthenticationCapacityExceeded(
                "authentication challenge capacity exhausted; retry after existing challenges expire"
            )
        outstanding_for_node = sum(
            1 for (existing_node, _session, _nonce), state in self._challenges.items()
            if existing_node == node_id and not state.consumed
        )
        if outstanding_for_node >= self.per_node_challenge_limit:
            raise AuthenticationCapacityExceeded(
                f"authentication challenge quota exhausted for node {node_id}"
            )
        nonce = self._nonce_source()
        self._require_text(nonce, "nonce")
        key = (node_id, session_id, nonce)
        # Reusing a verifier-issued nonce inside the live security window would
        # allow an earlier proof to become meaningful again. Reject it rather
        # than silently replacing the existing challenge.
        if key in self._challenges:
            raise AuthenticationError("authentication nonce reused inside validity window")
        challenge = AuthChallenge(node_id, session_id, nonce, now)
        self._challenges[key] = _ChallengeState(now)
        return challenge


    def abandon(self, node_id: str, session_id: str, nonce: str) -> bool:
        """Drop one unconsumed challenge owned by an abandoned handshake.

        Consumed entries are replay tombstones and deliberately remain until
        expiry. Only proof-less connection state is reclaimed early.
        """
        self._require_text(node_id, "node_id")
        self._require_text(session_id, "session_id")
        self._require_text(nonce, "nonce")
        key = (node_id, session_id, nonce)
        state = self._challenges.get(key)
        if state is None or state.consumed:
            return False
        del self._challenges[key]
        return True

    def prove(self, challenge: AuthChallenge) -> AuthProof:
        secret = self._secret(challenge.node_id)
        mac = hmac.new(secret, self._payload(challenge), hashlib.sha256).hexdigest()
        return AuthProof(
            challenge.node_id, challenge.session_id, challenge.nonce,
            challenge.issued_at, mac,
        )

    def verify(self, proof: AuthProof, *, expected_node_id: str, expected_session_id: str) -> None:
        if proof.node_id != expected_node_id or proof.session_id != expected_session_id:
            raise AuthenticationError("authenticated identity/session mismatch")
        secret = self._secret(expected_node_id)
        now = int(self._clock())
        if proof.issued_at > now + 5 or now - proof.issued_at > self.max_age_seconds:
            raise AuthenticationError("authentication proof expired")
        self._purge(now)
        key = (proof.node_id, proof.session_id, proof.nonce)
        state = self._challenges.get(key)
        if state is None:
            raise AuthenticationError("authentication proof has no current verifier-issued challenge")
        if state.issued_at != proof.issued_at:
            raise AuthenticationError("authentication proof timestamp disagrees with issued challenge")
        if state.consumed:
            raise ReplayDetected("authentication proof replayed")
        challenge = AuthChallenge(
            proof.node_id, proof.session_id, proof.nonce, proof.issued_at,
        )
        expected = hmac.new(secret, self._payload(challenge), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected, proof.mac_hex):
            raise AuthenticationError("invalid authentication proof")
        # Consume only after the MAC is valid. The tombstone remains until expiry
        # so replay protection is never shortened by capacity pressure.
        state.consumed = True
        self._challenges.move_to_end(key)

    def _purge(self, now: int) -> None:
        cutoff = now - self.max_age_seconds
        # Verification deliberately may reorder entries, so insertion/LRU order
        # is not an expiry order. The collection is already hard-bounded; scan it
        # completely and remove only records whose full replay window ended.
        for key, state in tuple(self._challenges.items()):
            if state.issued_at < cutoff:
                del self._challenges[key]

    def _secret(self, node_id: str) -> bytes:
        self._require_text(node_id, "node_id")
        try:
            return self._secrets[node_id]
        except KeyError as error:
            raise AuthenticationError(f"unknown node identity: {node_id}") from error

    @staticmethod
    def _require_text(value: str, name: str) -> None:
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{name} must be nonempty text")

    @staticmethod
    def _payload(challenge: AuthChallenge) -> bytes:
        fields = (
            challenge.node_id, challenge.session_id, challenge.nonce,
            str(challenge.issued_at),
        )
        return "\x00".join(fields).encode("utf-8", errors="strict")
