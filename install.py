"""Install dpr into the Python that runs this script:  python install.py"""
import shutil
import subprocess
import sys
from pathlib import Path


def main() -> int:
    here = Path(__file__).resolve().parent
    if sys.version_info < (3, 12):
        print(f"dpr needs Python 3.12 or newer; this is {sys.version.split()[0]}")
        return 2
    base = [sys.executable, "-m", "pip", "install", "-q", "--disable-pip-version-check"]
    scratch = [here / "build", here / "src" / "dpr_runtime.egg-info"]
    existed = {path for path in scratch if path.exists()}
    # A plain install first; a per-user one where the system Python refuses it.
    for extra in ([], ["--user"]):
        result = subprocess.run([*base, *extra, str(here)], capture_output=True, text=True)
        if result.returncode == 0:
            break
    for path in scratch:  # build leftovers do not belong in the source folder
        if path not in existed:
            shutil.rmtree(path, ignore_errors=True)
    if result.returncode != 0:
        lines = (result.stderr or result.stdout).strip().splitlines()
        print(f"install failed: {lines[-1] if lines else result.returncode}")
        print("install into a virtual environment instead:")
        print("  python -m venv .venv")
        print("  .venv\\Scripts\\activate" if sys.platform == "win32" else "  . .venv/bin/activate")
        print("  python install.py")
        return 2

    launcher = "dpr" if shutil.which("dpr") else f"{Path(sys.executable).stem} -m dpr"
    shown = subprocess.run(
        [sys.executable, "-c", f"from dpr.cli import welcome; print(welcome({launcher!r}))"],
        capture_output=True, text=True, cwd=str(Path.home()))
    if shown.returncode != 0:
        lines = shown.stderr.strip().splitlines()
        print(f"installed, but dpr does not start: {lines[-1] if lines else shown.returncode}")
        return 2
    print(shown.stdout, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
