"""Text that came from another machine, made safe to print or store.

Program output, error messages and machine names arrive from workers.  Printed as
they are, control and escape sequences could rewrite the screen, retitle or
reprogram the terminal, or hide text; bidirectional overrides could make text read
differently from what it is.  Only line breaks, tabs and (on screen) colour
survive.
"""
from __future__ import annotations

import re

_COLOUR = r"\x1b\[[0-9;]{0,32}m"
_ESCAPE = re.compile(
    r"\x1b\[[0-?]*[ -/]*[@-~]"          # CSI: cursor movement, erase, modes ...
    r"|\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)?"  # OSC: titles, hyperlinks, clipboard
    r"|\x1b[P^_X][^\x1b]*(?:\x1b\\)?"   # DCS, PM, APC, SOS strings
    r"|\x1b[@-Z\\-_]?"                  # other escapes, and a lone ESC
)
_CONTROL = re.compile("[\x00-\x08\x0b-\x1f\x7f-\x9f‪-‮⁦-⁩]")


def printable(text: str, *, colour: bool = True) -> str:
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    kept: list[str] = []
    position = 0
    coloured = False
    for match in _ESCAPE.finditer(text):
        kept.append(_CONTROL.sub("", text[position:match.start()]))
        if colour and re.fullmatch(_COLOUR, match.group(0)):
            kept.append(match.group(0))
            coloured = True
        position = match.end()
    kept.append(_CONTROL.sub("", text[position:]))
    result = "".join(kept)
    return result + "\x1b[0m" if coloured else result
