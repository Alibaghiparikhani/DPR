# dpr guide

dpr parallelizes ordinary Python programs across several computers on the same network,
without rewriting them for threads, processes or a distributed framework. It reads the
program, finds the parts that are independent of each other, and runs those parts at the
same time on different computers. This works up to a point: only the parts that dpr can
prove are self-contained, or that you mark with `@task`, run in parallel, and everything
else runs in order, as it would normally. The result is the same as running the program
with plain `python`, as long as the functions you mark with `@task` really are
self-contained and the program needs nothing outside its folder that the workers lack.
Programs with enough independent work simply finish sooner.

One computer is the **host**: it starts programs and hands out the work. The other
computers are **workers**: they run the work.

## Requirements

- Two or more computers on the same network
- Python 3.12 or newer on each computer
- `openssl` on the host (on Windows, the one included with Git for Windows is used)
- Windows, Linux or macOS. On Windows, use Git Bash.

## Install

On every computer, open a terminal in the folder that contains `install.py` and run:

```bash
python install.py
```

If the install is refused, the installer prints how to install into a virtual
environment instead.

## Start

On every computer:

```bash
dpr start
```

If `dpr` is not found, use `python -m dpr start`. This opens a session with a `dpr>`
prompt.

## Commands

| Command | Description |
|---|---|
| `host` | Start a cluster on this computer and show its join code |
| `join <code>` | Join a cluster (a computer that joined before can use `join` alone) |
| `run <file>` | Run a program on the cluster (host only) |
| `status` | Show the cluster |
| `approve <name>` | Let a waiting computer join (host only) |
| `kick <name>` | Turn away a waiting computer or remove a member (host only) |
| `exit` | Stop dpr on this computer |

Outside a session: `dpr start` and `dpr uninstall`.

## Set up a cluster

1. On the host, type `host`. It shows a join code starting with `dpr3_`.
2. On each worker, type `join <code>`. The worker shows `waiting for approval (123456)`.
3. The host shows `<name> wants to join (123456)`. If the numbers match, type
   `approve <name>` (or `approve 123456`).
4. Type `status` on the host to see the workers.

A worker waits up to five minutes for approval. Up to 32 computers can join one host.
On Windows, allow Python through the firewall on private networks when asked.

## Run a program

Put the program in a folder of its own, then on the host type:

```
dpr> run path/to/program.py
```

- The whole folder is sent to the workers (except caches, virtual environments, `.git`,
  `dpr-results` and credential files), so the program can read files in it using
  relative paths. Limits: 16 MiB in total, 8 MiB per file, 2048 files. A program
  directly in your home folder or at the top of a drive is refused.
- Output is printed on the host, followed by the run time. Only the first 8 KiB that
  each step prints is shown, and only for the first 1024 steps of a program. Each run is
  also saved in `dpr-results/` next to the program; the last 20 are kept.
- Ctrl-C cancels the run.
- Libraries the program imports (such as `numpy`) must be installed on every worker
  with `python -m pip install <name>`. Libraries installed with `--user` are not visible
  to programs run by dpr.

## How programs are split

dpr reads the program before running it and treats each top-level statement, apart from
function definitions, as a step.
Steps that don't depend on each other's results can run at the same time on different
workers. A step can only run on its own if it is self-contained: it takes its inputs as
arguments and returns its result. dpr checks simple functions itself; for other
functions, mark them with `@task`:

```python
from dag_runtime import task


@task
def count_primes(start, end):
    count = 0
    for n in range(max(start, 2), end):
        if all([n % d for d in range(2, int(n ** 0.5) + 1)]):
            count += 1
    return count


a = count_primes(0, 250_000)
b = count_primes(250_000, 500_000)
c = count_primes(500_000, 750_000)
d = count_primes(750_000, 1_000_000)

print("primes:", a + b + c + d)
```

The four `count_primes` calls run in parallel; the `print` runs after them. `@task` has
no effect when the file is run with plain `python`.

Things that limit parallelism:

- A top-level `import`, `print`, file operation, `for`, `while`, `if`, `with`, `try`,
  class definition or update such as `x += 1`: everything after it runs one step at a
  time. Put imports inside the `@task` functions, and printing at the end.
  (`from dag_runtime import task` is the one import that is fine at the top.)
- Several calls in one statement, or calls inside a loop or list comprehension, form a
  single step.
- Generator expressions (`sum(x for x in items)`), `async`, `yield`, or importing
  `threading`, `asyncio`, `multiprocessing` or `concurrent`, anywhere in the file: the
  whole program runs as a single step.

The program still runs correctly in all of these cases, just not in parallel.

## What works well

dpr helps most with programs that have many independent pieces of work, each taking
about a second or more, with little data passed between them: simulations, parameter
sweeps, number crunching over ranges, or processing many independent files. It does not
speed up programs whose steps each depend on the previous one, programs made of very
small operations, or programs that mostly move data.

## Update or uninstall

```bash
dpr uninstall        # answer y; removes dpr and its data
python install.py    # from the new version's folder, to update
```

After reinstalling, host again and have the workers join again.

## Troubleshooting

| Problem | Solution |
|---|---|
| `dpr: command not found` | Use `python -m dpr ...` |
| Prompt or Ctrl-C misbehaves in Git Bash | Start with `winpty python -m dpr start` |
| `dpr is already running on this machine` | Use the open session, or `exit` it first |
| `cannot reach ...` | Check both computers are on the same network and the firewall allows Python |
| `code no longer valid` | The host restarted; use the new code |
| `not approved in time` | Run `join` again and approve within five minutes |
| `no machines joined` | Wait until the workers show in `status` |
| `move ... into a folder of its own` | Move the program out of your home folder or drive root |
| `ModuleNotFoundError` | Install the library on every worker (not with `--user`) |
| The run gives no speedup | See "How programs are split" |
