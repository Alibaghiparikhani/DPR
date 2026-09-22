from __future__ import annotations

import pytest

from runtime_security import (
    AuthenticationCapacityExceeded, AuthenticationError, NodeAuthenticator, ReplayDetected,
)


def test_node_authentication_binds_node_session_and_nonce():
    now = [100]
    auth = NodeAuthenticator(
        {"W1": b"a" * 32}, clock=lambda: now[0], nonce_source=lambda: "nonce-1"
    )
    proof = auth.prove(auth.issue("W1", "session-7"))
    auth.verify(proof, expected_node_id="W1", expected_session_id="session-7")
    with pytest.raises(ReplayDetected):
        auth.verify(proof, expected_node_id="W1", expected_session_id="session-7")


def test_wrong_node_or_session_is_rejected():
    auth = NodeAuthenticator(
        {"W1": b"a" * 32, "W2": b"b" * 32},
        clock=lambda: 100,
        nonce_source=lambda: "nonce",
    )
    proof = auth.prove(auth.issue("W1", "s1"))
    with pytest.raises(AuthenticationError, match="identity/session"):
        auth.verify(proof, expected_node_id="W2", expected_session_id="s1")
    with pytest.raises(AuthenticationError, match="identity/session"):
        auth.verify(proof, expected_node_id="W1", expected_session_id="s2")


def test_tampered_and_expired_proofs_are_rejected():
    now = [100]
    auth = NodeAuthenticator(
        {"W1": b"a" * 32}, max_age_seconds=10,
        clock=lambda: now[0], nonce_source=lambda: "nonce"
    )
    proof = auth.prove(auth.issue("W1", "s"))
    tampered = type(proof)(proof.node_id, proof.session_id, proof.nonce, proof.issued_at, "00" * 32)
    with pytest.raises(AuthenticationError, match="invalid"):
        auth.verify(tampered, expected_node_id="W1", expected_session_id="s")
    now[0] = 111
    with pytest.raises(AuthenticationError, match="expired"):
        auth.verify(proof, expected_node_id="W1", expected_session_id="s")


def test_replay_window_is_bounded_without_evicting_unexpired_security_state():
    counter = [0]
    now = [100]

    def nonce():
        counter[0] += 1
        return f"n-{counter[0]}"

    auth = NodeAuthenticator(
        {"W1": b"a" * 32}, replay_limit=3, max_age_seconds=10,
        clock=lambda: now[0], nonce_source=nonce,
    )
    proofs = []
    for _ in range(3):
        proof = auth.prove(auth.issue("W1", "s"))
        auth.verify(proof, expected_node_id="W1", expected_session_id="s")
        proofs.append(proof)
    assert auth.replay_cache_size == 3

    # Security state is full: fail closed instead of evicting still-valid replay
    # tombstones. The oldest proof therefore remains non-replayable.
    with pytest.raises(AuthenticationCapacityExceeded):
        auth.issue("W1", "s")
    with pytest.raises(ReplayDetected):
        auth.verify(proofs[0], expected_node_id="W1", expected_session_id="s")

    # Once the entire validity window has elapsed, expired records can be purged
    # and admission resumes without weakening the prior window.
    now[0] = 111
    fresh = auth.prove(auth.issue("W1", "s"))
    auth.verify(fresh, expected_node_id="W1", expected_session_id="s")
    assert auth.replay_cache_size == 1


def test_verifier_rejects_proof_for_challenge_it_did_not_issue():
    verifier = NodeAuthenticator({"W1": b"a" * 32}, clock=lambda: 100)
    prover = NodeAuthenticator(
        {"W1": b"a" * 32}, clock=lambda: 100, nonce_source=lambda: "peer-nonce",
    )
    proof = prover.prove(prover.issue("W1", "s"))
    with pytest.raises(AuthenticationError, match="no current verifier-issued challenge"):
        verifier.verify(proof, expected_node_id="W1", expected_session_id="s")


def test_verifier_rejects_nonce_reuse_inside_live_security_window():
    auth = NodeAuthenticator(
        {"W1": b"a" * 32},
        clock=lambda: 100,
        nonce_source=lambda: "same-nonce",
        replay_limit=4,
    )
    auth.issue("W1", "s")
    with pytest.raises(AuthenticationError, match="nonce reused"):
        auth.issue("W1", "s")


def test_expiry_reclaims_consumed_challenge_even_after_verification_reorders_cache():
    now = [0]
    nonces = iter(("nonce-a", "nonce-b", "nonce-c"))
    auth = NodeAuthenticator(
        {"W1": b"a" * 32},
        max_age_seconds=60,
        replay_limit=2,
        clock=lambda: now[0],
        nonce_source=lambda: next(nonces),
    )
    first = auth.issue("W1", "s1")
    now[0] = 1
    auth.issue("W1", "s2")
    now[0] = 2
    auth.verify(auth.prove(first), expected_node_id="W1", expected_session_id="s1")

    # Verification moved the consumed first record behind the still-live second
    # record. Expiry reclamation must not depend on that cache ordering.
    now[0] = 61
    assert auth.replay_cache_size == 1
    fresh = auth.issue("W1", "s3")
    assert fresh.nonce == "nonce-c"
    assert auth.replay_cache_size == 2


def test_per_node_outstanding_quota_preserves_capacity_for_other_identity():
    counter = [0]
    def nonce():
        counter[0] += 1
        return f"n-{counter[0]}"
    auth = NodeAuthenticator(
        {"A": b"a" * 32, "B": b"b" * 32}, replay_limit=16,
        per_node_challenge_limit=2, clock=lambda: 100, nonce_source=nonce,
    )
    first = auth.issue("A", "s1")
    second = auth.issue("A", "s2")
    with pytest.raises(AuthenticationCapacityExceeded, match="quota"):
        auth.issue("A", "s3")
    # A cannot monopolize the global table; B still receives a challenge.
    assert auth.issue("B", "s1").node_id == "B"
    # Abandon immediately reclaims only unconsumed proof-less state.
    assert auth.abandon(first.node_id, first.session_id, first.nonce)
    assert auth.issue("A", "s4").node_id == "A"
    proof = auth.prove(second)
    auth.verify(proof, expected_node_id="A", expected_session_id="s2")
    assert not auth.abandon(second.node_id, second.session_id, second.nonce)
    with pytest.raises(ReplayDetected):
        auth.verify(proof, expected_node_id="A", expected_session_id="s2")
