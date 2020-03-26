import subprocess
from typing import BinaryIO


class WriterError(Exception):
    """Raised on any error encountered while writing to a target."""


def write_dump(target: str, stream: BinaryIO):
    """Writes a dump (as a byte stream) to the target database.

    Args:
        target: Target database URI.
        stream: Byte stream to read from.

    """
    process = subprocess.Popen(
        ['psql', '-f', '-', target],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=False,
        shell=False,
    )

    try:
        stdin = process.stdin
        for line in stream:
            stdin.write(line)
        _, stderr = process.communicate()
        if process.returncode > 0:
            raise WriterError(stderr.strip().decode('utf-8'))
    finally:
        stdin.close()
