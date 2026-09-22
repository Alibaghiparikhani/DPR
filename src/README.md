# dpr

A distributed Python runtime for machines on one LAN. One machine hosts; others join
and do the work. Correctness comes before parallelism: when Python behaviour or
mutable/native state cannot be proven safe to replay, dpr fails conservatively rather
than guessing.

## Install

```
python install.py
```

Python 3.12 or newer. `openssl` is needed on the machine that hosts (Git for Windows'
copy is found automatically).

## Use

```
dpr start       open a session
dpr uninstall   remove dpr and its data from this machine
```

Everything else happens inside the session:

```
host             start a cluster on this machine
join <code>      add this machine to a cluster
run <file>       run a Python program on the cluster
status           show the cluster
approve <name>   let a waiting machine join
kick <name>      remove a machine from the cluster
exit             stop everything and leave
```

A typical cluster: `host` on one machine prints a code; `join <code>` on each of the
others; `run analysis.py` on the host. Output is printed, and each run is kept as one
file in `dpr-results/` beside the program. Ctrl-C during `run` cancels the run.

Nobody joins without the host's say. A joining machine shows
`waiting for approval (482193)`; the host sees `laptop-b wants to join (482193)` and,
once the numbers match, types `approve laptop-b` (or `approve 482193`). `kick` turns a
waiting machine away, or removes a member: it is disconnected at once, its identity is
retired for good, and it can only come back by asking again after the host restarts
hosting. A name two machines share is told apart by the number (waiting) or the id
shown in `status` (members).

## What dpr handles itself

- **Processes.** Every node belongs to the session that started it and stops with it:
  on `exit`, and equally when the terminal is closed or the session is killed. Anything
  an earlier session left behind is cleared on the next `dpr start`. One session runs
  per machine.
- **Credentials.** Created on the first `host`, reused afterwards, renewed before they
  expire. A machine that joins again keeps its identity, and can rejoin with a bare
  `join`.
- **Ports.** A busy port is replaced by a free one.
- **Storage.** The package cache evicts old entries, runtime data is discarded when a
  worker restarts, logs rotate, run history keeps the latest 1000 runs, and
  `dpr-results/` keeps the latest 20 results per program.
- **Limits.** Per-task time, memory, CPU and file-size bounds are sized to the machine,
  so a runaway task cannot take it down and a real workload is not cut short.
- **Values and transfers.** Values live on disk, with room for a quarter of the free
  space (at least 256 MiB); one value can be up to 256 MiB. Each machine sends and
  receives four values at a time and the rest wait their turn on the host, so gathering
  many results onto one machine queues instead of failing. A transfer is given longer
  the larger it is, and one that stops moving is caught within seconds.
- **Bursts.** A machine that sends faster than the host keeps up is slowed down, not
  disconnected.

Nothing under `~/.dpr` (or `$DPR_HOME`) is meant to be edited; anything damaged there
is rebuilt.

## Security

**Who is trusted.** The host trusts every machine it lets join, and every joined
machine trusts the host: the host runs its programs there, as the user who typed
`join` (or as `nobody` when that user is root on Linux/macOS). Anyone who joins can
also influence the other workers, since values pass between them as Python objects.
So: host only among people you trust, keep the join code to them, and on a machine
you share, join from a separate OS account.

**What protects the cluster.**
- The host listens only on its LAN address and loopback, never on every interface,
  and says so if that address is reachable from the internet.
- Every connection is TLS 1.3 with certificates from a cluster-private CA, plus a
  replay-protected HMAC challenge. The CA key is destroyed once the identities are
  made. Running, inspecting and cancelling programs is allowed only from the host
  machine itself.
- A join code carries 96 random bits and a 128-bit pin of the host's certificate.
  The joining machine checks the pin before it sends anything. Wrong codes are
  answered slowly; the join service serves a few requests at a time, each with a
  hard deadline, and validates everything it receives.
- The code alone admits no one: the host approves each new machine after comparing a
  pairing number shown on both screens, and only the machine that asked can collect
  the approval. Removal is enforced by the host itself, which refuses the identity
  from then on, including after restarts; the removed machine is told so over its
  authenticated connection and forgets its credentials.
- Worker-to-worker transfers use their own mutual TLS, are authorised per transfer,
  and are verified by size and SHA-256.
- Output, errors and machine names that come from other machines are stripped of
  terminal control sequences before they are shown or saved.

**What protects this machine.**
- `~/.dpr` is private to the user. dpr refuses a `$DPR_HOME` that already holds other
  files, and deletes only its own entries, so a mistyped path cannot cost data.
- The folder sent with a program never includes credential stores (`.ssh`, `.aws`,
  `.gnupg`, `.kube`, `.docker`, `.netrc`, SSH keys, ...). A program in your home
  folder or a drive root is refused: move it into a folder of its own.
- Submitted code gets a scrubbed environment and time, memory, CPU, process and
  file-size limits, and every process it starts is killed with it.
- Nodes take secrets over a pipe, never on the command line or in the environment,
  and exit with the session.

This is defence in depth for a trusted group, not a sandbox for strangers' code.

## Architecture

```text
                         coordinator (host)
                  authoritative control plane
                         TLS + HMAC
             +---------------+---------------+
             |               |               |
          worker 1        worker 2        worker 3
          packages         packages         packages
          executors        executors        executors
          contexts         contexts         contexts
             \===============|===============/
                  direct authenticated P2P data
```

The coordinator never executes user Python and does not relay bulk runtime values.
Execution modes:

- **isolated candidates**: one-shot child processes, retried only where the
  coordinator contract permits;
- **shared contexts**: persistent worker subprocesses that preserve in-context state;
- **native regions**: persistent conservative state; uncertain mutation is never
  replayed automatically.

Physical exactly-once execution is not claimed. Stale or duplicate attempts are fenced
so at most one logical attempt commits.

## Failure model

- a logical task or run failure does not prove physical work has stopped;
- coordinator capacity stays reserved while unresolved physical work exists;
- stale sessions, attempts, context results and transfers are fenced by generation;
- isolated work follows the coordinator's safe-retry contract;
- started shared/native work is not replayed after uncertainty;
- transfer retry is distinct from task retry.

Run history is diagnostic, not crash recovery.

## Platforms

Windows, Linux and macOS. Package paths are validated against Windows rules on every
platform. Descendant cleanup is strongest on Linux (process groups and parent-death
signals); macOS uses a parent-PID watcher and Windows a Job Object.

Not included: active-run coordinator crash recovery, distributed durable storage,
service discovery, untrusted-member sandboxing.

## Internals

`dpr/` is the product layer: `cli` (the two commands), `session` (the prompt),
`cluster` (host and worker roles), `node` (the background processes), `runs`,
`enroll` (join codes), `credentials`, `processes`, `home` and `text` (making remote
text safe to print). The runtime beneath it
lives in `coordinator/`, `worker/`, `scheduler/`, `execution/`, `dag_runtime/`,
`networking/`, `protocol/`, `program_package/` and `runtime_security/`, each with its
own contract document.
