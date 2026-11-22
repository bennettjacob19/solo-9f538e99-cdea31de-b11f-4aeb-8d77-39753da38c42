"""LSN (Log Sequence Number) parsing and WAL segment arithmetic.

PostgreSQL represents an LSN as ``X/YYZZZZZZ`` (hex).  A WAL segment is a
contiguous, fixed-size range of LSNs.  With the default 16 MiB segment size a
segment starts every 2^24 bytes, which is why segment file names only use the
low 24 bits of the byte offset.
"""

from __future__ import annotations

import re

MAX_LSN = (1 << 64) - 1
# PostgreSQL accepted segment sizes are powers of two between 1 MiB and 1 GiB.
MIN_SEGMENT_SIZE = 1 * 1024 * 1024
MAX_SEGMENT_SIZE = 1 * 1024 * 1024 * 1024
DEFAULT_SEGMENT_SIZE = 16 * 1024 * 1024

_LSN_RE = re.compile(r"^[0-9A-Fa-f]+/[0-9A-Fa-f]+$")


def parse_lsn(value: int | str) -> int:
    """Parse an LSN given either as an unsigned integer or as ``X/Y`` hex text."""
    if isinstance(value, bool):  # bool is a subclass of int – reject explicitly
        raise ValueError("LSN must be an integer or 'X/Y' hex string")
    if isinstance(value, int):
        lsn = value
    elif isinstance(value, str):
        text = value.strip()
        if not _LSN_RE.match(text):
            raise ValueError(f"invalid LSN text {value!r}, expected 'X/Y' hex form")
        high, low = text.split("/")
        lsn = (int(high, 16) << 32) | int(low, 16)
    else:
        raise ValueError("LSN must be an integer or 'X/Y' hex string")
    if lsn < 0 or lsn > MAX_LSN:
        raise ValueError(f"LSN {value!r} out of uint64 range")
    return lsn


def format_lsn(lsn: int) -> str:
    """Render an integer LSN in PostgreSQL's ``X/YYYYYYYY`` text form."""
    return f"{lsn >> 32:X}/{lsn & 0xFFFFFFFF:08X}"


def validate_segment_size(size: int) -> None:
    if not isinstance(size, int) or isinstance(size, bool):
        raise ValueError("segment_size must be an integer (bytes)")
    if size < MIN_SEGMENT_SIZE or size > MAX_SEGMENT_SIZE:
        raise ValueError(
            f"segment_size must be between {MIN_SEGMENT_SIZE} and "
            f"{MAX_SEGMENT_SIZE} bytes"
        )
    if size & (size - 1) != 0:
        raise ValueError("segment_size must be a power of two")


def segment_start(lsn: int, segment_size: int) -> int:
    """Return the first LSN of the segment that contains ``lsn``."""
    return lsn - (lsn % segment_size)


def next_segment_start(lsn: int, segment_size: int) -> int:
    """Return the first LSN of the segment after the one containing ``lsn``."""
    return segment_start(lsn, segment_size) + segment_size


def highest_reclaimable_segment_start(
    cutoff_lsn: int, segment_size: int
) -> int | None:
    """Highest segment whose *entire* range is below ``cutoff_lsn``.

    A segment is fully reclaimable only when its end LSN (``start + size``) is
    not greater than the cutoff.  When no complete segment qualifies, ``None``
    is returned – the caller must keep every segment in the current snapshot.
    """
    if cutoff_lsn < segment_size:
        return None
    # Floor to the segment grid, then drop one more segment whenever the
    # cutoff lands exactly on a boundary: the segment *ending* at the cutoff
    # is still in use by whichever protection point sits on that LSN.
    candidate = (cutoff_lsn // segment_size - 1) * segment_size
    if candidate < 0:
        return None
    return candidate
