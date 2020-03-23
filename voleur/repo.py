import json

from voleur import models
from voleur import storage


class VersionConflict(Exception):
    """Raised when an out-of-date stash is saved."""


class StashRepo:
    """Repo for loading/saving stashes."""

    _METADATA_FILENAME = '_metadata.json'

    @classmethod
    def load(cls, bucket: str) -> models.Stash:
        """Load the stash in the given bucket.

        Args:
            bucket: The stash bucket.

        Returns:
            Stash

        """
        try:
            metadata_path = cls._get_metadata_path(bucket)
            content = storage.read('s3', metadata_path)
            stash = unmarshal_stash(json.loads(content))
        except storage.NotFoundError:
            stash = models.Stash(bucket=bucket)

        return stash

    @classmethod
    def save(cls, stash: models.Stash) -> models.Stash:
        """Save the stash.

        Args:
            stash: The stash to save.

        Returns:
            Stash

        """
        content = json.dumps(marshal_stash(stash))
        metadata_path = cls._get_metadata_path(stash.bucket)
        storage.store('s3', metadata_path, content)
        return stash

    @classmethod
    def _get_metadata_path(cls, bucket: str):
        return f'{bucket}/{cls._METADATA_FILENAME}'


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Object (un)marshalling
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


def marshal_stash(s: models.Stash) -> dict:
    """Returns a dict from a `Stash` instance.

    Args:
        s: Stash instance.

    Returns:
        dict

    """
    dumps = [marshal_dump(d) for d in s.dumps]
    return {'bucket': s.bucket, 'tags': s.tags, 'dumps': dumps}


def unmarshal_stash(s: dict) -> models.Stash:
    """Returns a `Stash` instance from a stash dict.

    Args:
        stash: Stash dict.

    Returns:
        models.Stash

    """
    s['dumps'] = [unmarshal_dump(d) for d in s['dumps']]
    return models.Stash(**s)


def marshal_dump(d: models.Dump) -> dict:
    """Returns a dict from a `Dump` instance.

    Args:
        d: Dump instance.

    Returns:
        dict

    """
    return {
        'dump_id': d.dump_id,
        'storage_url': d.storage_url,
        'timestamp': d.timestamp,
    }


def unmarshal_dump(d: dict) -> models.Dump:
    """Returns a `Dump` instance from a dump dict.

    Args:
        d: Dump dict.

    Returns:
        models.Dump

    """
    return models.Dump(**d)
