import io
import os
import subprocess
import contextlib
import platform
from typing import ContextManager, Iterator, List, Optional, cast


DEFAULT_KLEPTO_CONFIG = 'klepto.toml'
KLEPTO_VERSION = '0.0.9'


class DumperError(Exception):
    """Raised on any error encountered while dumping 💩"""


def extract_dump(
    source_uri: str, klepto_config: Optional[str] = None,
) -> ContextManager[io.BufferedReader]:
    """Extracts and anonymizes a dump from the source database.

    Returns a context manager which yields a stream containing the data (as bytes) from
    the extracted dump.

    Args:
        source_uri: Source database URI.
        klepto_config (optional): Path to a klepto config file

    Returns:
        ContextManager[IO[bytes]]

    """
    if not klepto_config:
        klepto_config = DEFAULT_KLEPTO_CONFIG
    _validate_klepto_config(klepto_config)
    return _klepto_steal(source_uri, config=klepto_config)


@contextlib.contextmanager
def _klepto_steal(from_uri: str, *, config: str) -> Iterator[io.BufferedReader]:
    """Runs klepto and streams its output.

    Args:
        from_uri: Source database URI.
        config: Path to klepto config file.

    Raises:
        DumperError: If there's an error in running the klepto command,

    Yields:
        io.BufferedReader: A stream to read the output from klepto.

    """
    try:
        args = _get_popen_args(from_uri, config)
        process = subprocess.Popen(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=False,
            text=False,
        )
    except FileNotFoundError as e:
        raise DumperError(e)

    stdout = process.stdout
    stderr = process.stderr

    try:
        err_msg = _extract_stderr_err_msg(stderr)
        if err_msg:
            raise DumperError(err_msg)
        yield cast(io.BufferedReader, stdout)
    finally:
        stdout.close()
        stderr.close()


def _validate_klepto_config(config: str):
    """Validates the klepto config path.

    Raises:
        DumperError: On invalid path.

    """
    if not os.path.exists(config):
        raise DumperError(f'klepto config ({config}) was not found')
    if not os.path.isfile(config):
        raise DumperError(f'klepto config ({config}) needs to be a file')


def _extract_stderr_err_msg(stderr) -> Optional[str]:
    """Reads `stderr` output and tries to extract an error for determining whether the
    klepto command has failed.

    Args:
        stderr: stderr stream.

    Returns:
        Optional[str]: An error string if there was an error or None.

    """
    lines = stderr.readlines()
    if not lines:
        return None
    head = lines[0].decode('utf-8')
    return head if head.lower().startswith('error:') else None


def _get_popen_args(from_uri: str, config: str) -> List[str]:
    """Gets the arguments for running klepto.

    Args:
        from_uri: Source database URI.
        config: Path to klepto config file.

    Returns:
        list: List of arguments.

    """
    return [
        _build_klepto_path(),
        'steal',
        '--from',
        from_uri,
        '--to',
        'os://stdout/',
        '--read-max-conns',
        '10',
        '--concurrency',
        '4',
        '--read-timeout',
        '20m',
        '--config',
        config,
    ]


def _build_klepto_path() -> str:
    """Builds the path to the platform-specific klepto binary.

    Returns:
        str

    """
    system = platform.system().lower()
    vendor_rel_path = f'../vendor/klepto/{KLEPTO_VERSION}/klepto_{system}_amd64'
    dirname = os.path.dirname(__file__)
    return os.path.join(dirname, vendor_rel_path)
