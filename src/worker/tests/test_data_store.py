from __future__ import annotations

from pathlib import Path
import hashlib
import pytest

import protocol as p
from scheduler import DataForm
from worker import DataStoreConflict, DataStoreLimits, LocalDataStore


def ref(state=None):
    return p.DataReference("a"*64,"run","value",DataForm.OBJECT_SNAPSHOT if state else DataForm.IMMUTABLE_VALUE,state)


def test_exact_identity_is_immutable_and_session_scoped(tmp_path):
    store=LocalDataStore(tmp_path/"data")
    data=ref()
    first=store.publish_bytes(data,b"one",session_id="s1")
    assert store.get(data,session_id="s1")==first
    assert store.get(data,session_id="s2") is None
    with pytest.raises(DataStoreConflict):
        store.publish_bytes(data,b"two",session_id="s1")
    with pytest.raises(DataStoreConflict):
        store.publish_bytes(data,b"one",session_id="s2")


def test_exact_object_state_versions_are_distinct_physical_keys(tmp_path):
    store=LocalDataStore(tmp_path/"data")
    one=ref("state-1"); two=ref("state-2")
    store.publish_bytes(one,b"same",session_id="s")
    store.publish_bytes(two,b"same",session_id="s")
    assert store.item_count==2
    assert store.get(one,session_id="s").path != store.get(two,session_id="s").path


def test_metadata_lookup_is_o1_but_explicit_integrity_verification_detects_corruption(tmp_path):
    store=LocalDataStore(tmp_path/"data")
    data=ref(); entry=store.publish_bytes(data,b"trusted",session_id="s")
    entry.path.write_bytes(b"corrupt")
    # F10 deliberately removed whole-file hashing from metadata lookup; child-side
    # reads validate the certified size/digest before deserializing.
    assert store.get(data,session_id="s") == entry
    assert store._verify_entry(entry) is False


def test_new_worker_store_instance_purges_unindexed_prior_session_and_staging(tmp_path):
    root=tmp_path/"data"
    store=LocalDataStore(root)
    data=ref(); entry=store.publish_bytes(data,b"old-session",session_id="s1")
    stale=root/"staging"/"interrupted.part"; stale.write_bytes(b"partial")
    assert entry.path.exists() and stale.exists()
    # F52 holds an exclusive directory lock while a worker is live. A replacement
    # process may reclaim DPR-owned stale content only after the prior owner exits.
    store.close()
    replacement=LocalDataStore(root)
    assert replacement.item_count==0
    assert not any((root/"content").iterdir())
    assert not any((root/"staging").iterdir())
    assert replacement.get(data,session_id="s2") is None


def test_store_capacity_is_explicitly_bounded(tmp_path):
    store=LocalDataStore(tmp_path/"data",limits=DataStoreLimits(max_bytes=5,max_items=1,max_value_bytes=5))
    store.publish_bytes(ref(),b"12345",session_id="s")
    with pytest.raises(Exception,match="limit|bound"):
        store.publish_bytes(p.DataReference("a"*64,"run","other",DataForm.IMMUTABLE_VALUE),b"x",session_id="s")


def test_foreign_directory_error_names_the_path_and_the_fix(tmp_path):
    """The guard exists because two workers sharing a directory silently wiped live
    data; the message has to say which directory and what to do about it."""
    import pytest as _pytest
    from worker.data_store import DataStoreIntegrityError

    target = tmp_path / "my-project"
    target.mkdir()
    (target / "notes.txt").write_text("user data", encoding="utf-8")

    with _pytest.raises(DataStoreIntegrityError) as excinfo:
        LocalDataStore(target)
    message = str(excinfo.value)
    assert str(target) in message
    assert "notes.txt" in message
    assert "--data-dir" in message
    assert (target / "notes.txt").exists(), "guard must not delete anything"


def test_interrupted_marker_initialization_is_repaired(tmp_path):
    """A force-killed worker could leave a zero-byte ownership marker.

    Creating the marker used to be create-then-write, so a kill in between left an
    empty file and every later start refused the directory as foreign.  An empty
    marker is an interrupted initialization, not someone else's data.
    """
    from worker.data_store import DataStoreIntegrityError, _DATA_MARKER, _DATA_MARKER_BYTES

    root = tmp_path / "data"
    store = LocalDataStore(root)
    store.close()
    marker = root / _DATA_MARKER
    marker.write_bytes(b"")

    repaired = LocalDataStore(root)
    repaired.close()
    assert marker.read_bytes() == _DATA_MARKER_BYTES

    marker.write_bytes(b"not ours\n")
    with pytest.raises(DataStoreIntegrityError) as excinfo:
        LocalDataStore(root)
    assert str(root) in str(excinfo.value)
