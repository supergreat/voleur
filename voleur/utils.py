import uuid
import io
from datetime import datetime
from typing import Iterable, Iterator
import queue
import threading


def generate_dump_filename() -> str:
    """Generates a unique dump filename.

    Returns:
        str: The filename.

    """
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    return f'{uuid.uuid4().hex}_{timestamp}.dump'


class _IteratorStream(io.RawIOBase):
    """Helper class that implements the stream interface ontop of an iterator that
    yields bytestrings.

    """

    def __init__(self, iterator: Iterator[bytes]):
        self._leftover = b''
        self._iterator = iterator

    def readable(self):
        return True

    def readinto(self, b) -> int:
        """Read bytes into a pre-allocated, writable bytes-like object b, and return the
        number of bytes read. For example, b might be a bytearray.

        Returns:
            int: bytes read

        """
        bytes_to_read = len(b)

        try:
            chunk = self._leftover or next(self._iterator)
        except StopIteration:
            # We've exhausted the iterator and we don't have any leftovers so we
            # indicate EOF.
            return 0

        output, self._leftover = chunk[:bytes_to_read], chunk[bytes_to_read:]
        b[: len(output)] = output
        return len(output)


def iterator_to_stream(iterator: Iterator) -> io.BufferedReader:
    """Constructs a `BufferedReader` from an iterator that yields byte strings.

    Args:
        iterator: The iterator to wrap.

    Returns:
        io.BufferedReader

    """
    stream = _IteratorStream(iterator)
    return io.BufferedReader(stream)


class FileReaderThread(threading.Thread):
    """Helper thread for reading file output given one of its open streams. Reading
    in threads avoids the possibility of a deadlock when reading both stdin and stdout.

    """

    def __init__(self, fd):
        self._fd = fd
        self._queue = queue.Queue()
        self._stop_event = threading.Event()
        super().__init__()

    def run(self):
        """Run the thread. Read the file line by line."""
        while not self.is_stopped():
            for line in self._fd:
                self._queue.put(line)
            else:
                self.stop()

    def stop(self):
        """Stop the thread."""
        self._stop_event.set()

    def is_stopped(self) -> bool:
        """Return True if the thread is stopped."""
        return self._stop_event.is_set()

    def eof(self):
        """EOF if the thread is dead and there are no more lines in the queue."""
        return not self.is_alive() and self._queue.empty()

    def iter_lines(self) -> Iterable[bytes]:
        """Yields lines from the stream."""
        while not self._queue.empty():
            yield self._queue.get()
