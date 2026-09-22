# Deterministic program packages

`program_package` provides the content-addressed package format used by real
isolated workers. It is deliberately separate from runtime object transfer; the real worker-to-worker data plane uses its own transfer representation and does not reuse this cache/archive format.

## Identity and deterministic construction

A package manifest contains `format_version` plus an ordered list of regular
files. Each entry records a portable POSIX-relative path, byte length, and
lowercase SHA-256. `package_id` is SHA-256 over the canonical JSON identity
record containing exactly that version and file list. Source-root paths,
mtimes, archive timestamps, filesystem enumeration order, and temporary paths
are not identity inputs. The builder sorts paths, fixes ZIP timestamps and file
modes, and produces byte-identical archives for identical included bytes.

Generated/VCS/editor state (`__pycache__`, `.pytest_cache`, `.git`, common type
checker/linter caches, `.pyc`, `.pyo`, swap/backup files) is excluded. Symlinks
and non-regular filesystem entries are rejected rather than followed.

## Archive and cache safety

The worker cache is content-addressed as `entries/<package_id>/` with verified
content under `content/` and a canonical manifest copy. Downloads and extraction
happen only in `staging/`; publication uses an atomic rename after complete
manifest, path, size and SHA-256 verification. A corrupt published entry is
removed and is never advertised as prepared.

Extraction rejects absolute/traversing/drive-qualified/NUL/backslash paths,
duplicate members, file/directory prefix collisions, symlinks, non-regular ZIP
members, missing/extra members, oversized manifests, excessive member counts,
per-file/unpacked/archive limits and digest mismatches. Reads are bounded while
decompressing. Stale staging state is cleaned on cache construction.

The cache does not evict in-use content in Batch 2. Instead it has an explicit
maximum byte budget and fails preparation with cache-full/resource failure when
new content would exceed it. This is intentionally simpler than an unsafe LRU.

## Preparation and reuse

The coordinator-side `PackageRepository` is bounded and immutable by
`package_id`. Package bytes are chunked over the existing authenticated Batch-1
control connection. A worker does not report `ProgramPrepared` merely because
bytes arrived: it installs and verifies the package, verifies the entrypoint
source hash, re-analyzes/lowers the source, and requires the reconstructed
`ProgramIdentity` and `ExecutionPlan.id` to match the requested identities.

Concurrent preparation requests for the same immutable package use a worker
single-flight installation. The transport also suppresses duplicate package-byte
streams while one preparation for that package is in flight on the connection.
A later failed preparation can be retried.
