import abc
import io
import contextlib
from typing import Iterator, ContextManager, BinaryIO, cast

import boto3
from botocore import exceptions as botocore_exc

from voleur import utils


class StorageError(Exception):
    """Generic storage error."""


class NotFoundError(StorageError):
    """Raised when a file is not found."""


class InvalidStorageURL(StorageError):
    """Raised when an operation involves a storage url which cannot be parsed."""


class StorageBackendNotSupported(StorageError):
    """Raised when a backend is requested but is not supported."""


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Storage engine interface
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


def store(backend: str, path: str, content: str) -> str:
    """Stores the content at the given path.

    Args:
        backend: The storage backend to use.
        path: The storage path.
        content: The content to store.

    Raises:
        StorageBackendNotSupported

    Returns:
        str: Storage URL.

    """
    path = get_backend(backend).store(path, content)
    return make_storage_url(backend, path)


def store_stream(backend: str, path: str, stream: BinaryIO) -> str:
    """Stores the contents of the stream at the given path.

    Args:
        backend: The storage backend to use.
        path: The storage path.
        stream: Bytes stream to read the content from.

    Raises:
        StorageBackendNotSupported

    Returns:
        str: Storage URL.

    """
    path = get_backend(backend).store_stream(path, stream)
    return make_storage_url(backend, path)


def read(backend: str, path: str) -> str:
    """Reads the contents at the given path.

    Args:
        backend: The storage backend to use.
        path: The storage path.

    Raises:
        NotFoundError
        StorageBackendNotSupported

    Returns:
        str: The contents.

    """
    return get_backend(backend).read(path)


def stream(backend: str, path: str) -> ContextManager[BinaryIO]:
    """Context-manager for streaming the contents at the given path.

    Args:
        backend: The storage backend to use.
        path: The storage path.

    Raises:
        StorageBackendNotSupported

    Yields:
        BinaryIO: Bytes stream to read from.

    """
    return get_backend(backend).stream(path)


def read_storage_url(storage_url: str) -> str:
    """Reads the contents at a storage URL.

    Args:
        backend: The storage backend to use.
        path: The storage path.

    Returns:
        str: The contents.

    """
    backend, path = parse_storage_url(storage_url)
    return read(backend, path)


def stream_storage_url(storage_url: str) -> ContextManager[BinaryIO]:
    """Streams the contents at a storage URL.

    Args:
        backend: The storage backend to use.
        path: The storage path.

    Raises:
        InvalidStorageURL
        StorageBackendNotSupported

    Yields:
        IO[bytes]: Bytes stream to read from.

    """
    backend, path = parse_storage_url(storage_url)
    return stream(backend, path)


def parse_storage_url(storage_url: str) -> tuple:
    """Parses a storage url to its `backend` and `path` constituents.

    Raises:
        InvalidStorageURL

    Returns
        tuple: (backend, path)

    """
    tokens = storage_url.split('://')
    if len(tokens) != 2:
        raise InvalidStorageURL(storage_url)
    return tuple(tokens)


def make_storage_url(backend: str, path: str) -> str:
    """Returms a storage url from its `backend` and `path` constituents.

    Args:
        backend: Backend name.
        path: File path.

    Raises:
        StorageBackendNotSupported

    Returns
        str: Storage URL.

    """
    if not is_backend_supported(backend):
        raise StorageBackendNotSupported(backend)
    return f'{backend}://{path}'


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Abstract storage backend
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class StorageBackend(abc.ABC):
    """Storage backend interface."""

    @abc.abstractmethod
    def store(self, path: str, content: str) -> str:
        """Stores the contents at the given path.

        Args:
            path: The storage path.
            content: Contents to store.

        Returns:
            str: The file path.

        """

    @abc.abstractmethod
    def store_stream(self, path: str, stream: BinaryIO) -> str:
        """Stores the contents of the stream at the given path.

        Args:
            path: The storage path.
            stream: Bytes stream to read the content from.

        Returns:
            str: The file path.

        """

    @abc.abstractmethod
    def read(self, path: str) -> str:
        """Reads the contents at the given path.

        Args:
            path: The storage path.

        Raises:
            NotFoundError

        Returns:
            str: The contents.

        """

    @abc.abstractmethod
    @contextlib.contextmanager
    def stream(self, path: str) -> Iterator[BinaryIO]:
        """Context-manager for streaming the contents at the given path.

        Args:
            path: The storage path.

        Raises:
            NotFoundError

        Yields:
            BinaryIO: Bytes stream to read from.

        """


def is_backend_supported(name: str) -> bool:
    """Returns if the backend is supported.

    Args:
        name: The backend name.

    Returns:
        bool

    """
    return name == 's3'


def get_backend(name: str) -> StorageBackend:
    """Returns a backend instance from its name.

    Args:
        name: The backend name.

    Returns:
        StorageBackend

    """
    if not is_backend_supported(name):
        raise StorageBackendNotSupported(name)
    return S3()


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Concrete storage backends
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class S3(StorageBackend):
    """Storage backend using S3."""

    _ENCODING = 'utf-8'
    name: str = 's3'

    def __init__(self):
        self._client = boto3.client('s3')

    def store(self, path: str, text: str) -> str:
        bucket, key = self._parse_path(path)
        body = text.encode(self._ENCODING)
        self._client.put_object(Body=body, Bucket=bucket, Key=key)
        return path

    def store_stream(self, path: str, stream: BinaryIO) -> str:
        bucket, key = self._parse_path(path)
        self._client.upload_fileobj(stream, bucket, key)
        return path

    def read(self, path: str) -> str:
        bucket, key = self._parse_path(path)
        fileobj = io.BytesIO()

        try:
            self._client.download_fileobj(bucket, key, fileobj)
        except botocore_exc.ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                raise NotFoundError(path)
            raise

        fileobj.seek(0)
        return fileobj.read().decode(self._ENCODING)

    @contextlib.contextmanager
    def stream(self, path: str) -> Iterator[BinaryIO]:
        reader = None

        bucket, key = self._parse_path(path)
        resp = self._client.get_object(Bucket=bucket, Key=key)

        try:
            reader = utils.iterable_to_stream(resp['Body'].iter_lines())
            yield cast(BinaryIO, reader)
        finally:
            if reader:
                reader.close()

    def _parse_path(self, path: str) -> tuple:
        bucket, key = path.split('/', maxsplit=1)
        return bucket, key
