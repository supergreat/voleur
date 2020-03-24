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
        while not stream.closed:
            stdin.write(_fix_insert_statement(stream.readline()))
        _, stderr = process.communicate()
        if process.returncode > 0:
            raise WriterError(stderr.strip().decode('utf-8'))
    finally:
        stdin.close()


def _fix_insert_statement(line: bytes) -> bytes:
    """Necessary hack to fix sql statements because `klepto` emits invalid SQL statements
    for `psql`.

    Args:
        line: The line to check and fix.

    Returns:
        bytes: Fixed line.

    """
    if not line.startswith(b'INSERT INTO'):
        return line

    line = line.strip()
    line = line.replace(b'INSERT INTO ', b'INSERT INTO public.')
    line = line.replace(b'\'NULL\'', b'NULL')
    line = line.replace(b'+0000 UTC', b'+00')

    if not line.endswith(b';'):
        line = line + b';'

    return line + b'\n'
