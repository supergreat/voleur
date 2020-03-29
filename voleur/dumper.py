import os
import subprocess
import contextlib
import platform
from typing import ContextManager, Iterator, List, Optional, BinaryIO, cast

from voleur import utils


DEFAULT_KLEPTO_CONFIG = 'klepto.toml'
KLEPTO_VERSION = '0.1'
ERR_SYMBOL = '⨯'


class DumperError(Exception):
    """Raised on any error encountered while dumping 💩"""


def extract_dump(
    source_uri: str, klepto_config: Optional[str] = None,
) -> ContextManager[BinaryIO]:
    """Extracts and anonymizes a dump from the source database.

    Returns a context manager which yields a stream containing SQL statements for
    recostructing the extracted database structure and rows.

    Args:
        source_uri: Source database URI.
        klepto_config (optional): Path to a klepto config file

    Returns:
        ContextManager[BinaryIO]

    """
    if not klepto_config:
        klepto_config = DEFAULT_KLEPTO_CONFIG
    _validate_klepto_config(klepto_config)
    return _klepto_steal(source_uri, config=klepto_config)


@contextlib.contextmanager
def _klepto_steal(from_uri: str, *, config: str) -> Iterator[BinaryIO]:
    """Runs klepto and streams its output.

    Args:
        from_uri: Source database URI.
        config: Path to klepto config file.

    Raises:
        DumperError: If there's an error in running the klepto command.

    Yields:
        BinaryIO: A stream to read the output from klepto.

    """
    try:
        args = _get_popen_args(from_uri, config)
        proc = subprocess.Popen(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=False,
            text=False,
        )
    except FileNotFoundError as e:
        raise DumperError(e)

    stdout = proc.stdout
    stderr = proc.stderr

    try:
        iterator = _consume_output(stdout, stderr)
        stream = utils.iterator_to_stream(iterator)
        yield cast(BinaryIO, stream)
    finally:
        stdout.close()
        stderr.close()


def _consume_output(stdout, stderr) -> Iterator[bytes]:
    """Consumes output from stdin and stderr. Checks stderr for klepto error output
    and stdin for invalid SQL statements to fix.

    Args:
        stdout
        stderr

    Raises:
        DumperError: If error output is encountered.

    Returns:
        Iterator[bytes]: Yields fixed bytestrings.

    """
    err_msg = None

    # Read output in two threads to avoid deadlocks.
    stdout_reader = utils.FileReaderThread(stdout)
    stdout_reader.start()
    stderr_reader = utils.FileReaderThread(stderr)
    stderr_reader.start()

    while not (stdout_reader.eof() and stderr_reader.eof()):
        for line_bytes in stderr_reader.iter_lines():
            line_string = line_bytes.strip().decode('utf-8')

            # Print stderr output since it contains informational messages.
            print('klepto:', line_string)

            if ERR_SYMBOL in line_string[:15]:
                # On error, stop readers, set err flag. Stopping the readers will
                # eventually exit this loop.
                stdout_reader.stop()
                stderr_reader.stop()
                err_msg = line_string

        for line_bytes in stdout_reader.iter_lines():
            yield _process_stdout_line(line_bytes)

    # Be nice and join the threads.
    stdout_reader.join()
    stderr_reader.join()

    # After everything has been tidied up, raise the error if any.
    if err_msg:
        raise DumperError(err_msg)


def _process_stdout_line(line: bytes) -> bytes:
    """Processes a line of stdout from Klepto.

    Args:
        line: Klepto output line.

    Returns:
        bytes

    """
    if line.startswith(b'INSERT INTO'):
        line = line.strip()
        line = line.replace(b'INSERT INTO ', b'INSERT INTO public.')
        line = line.replace(b'\'NULL\'', b'NULL')
        line = line.replace(b'+0000 UTC', b'+00')
        if not line.endswith(b';'):
            line += b';\n'
    return line


def _validate_klepto_config(config: str):
    """Validates the klepto config path.

    Raises:
        DumperError: On invalid path.

    """
    if not os.path.exists(config):
        raise DumperError(f'klepto config ({config}) was not found')
    if not os.path.isfile(config):
        raise DumperError(f'klepto config ({config}) needs to be a file')


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
