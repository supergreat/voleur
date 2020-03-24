import functools
from typing import Callable, Any, List

from voleur import cli
from voleur import storage
from voleur import repo
from voleur import utils
from voleur import models
from voleur import dumper
from voleur import writer


def stash(env: cli.Env):
    """Runs the `stash` CLI command,.

    Args:
        env: CLI environment.

    """
    source = env.get_arg('<source>')
    bucket = env.get_arg('-b')
    tags = env.get_arg('-t')
    klepto_config = env.get_arg('-c')

    env.info('💭 Extracting dump...')

    with dumper.extract_dump(source, klepto_config=klepto_config) as stream:
        path = f'{bucket}/{utils.generate_dump_filename()}'
        storage_url = storage.store_stream('s3', path, stream)

    env.info(f'💩 Dump extracted: {storage_url}')

    stash = repo.StashRepo.load(bucket)
    update_fn = functools.partial(_add_dump, storage_url, tags=tags)
    dump = _safely_update_stash(update_fn, stash)

    env.ok(f'✅ Dump stashed: id: {dump.dump_id}, tags: {stash.get_tags(dump.dump_id)}')


def restore(env: cli.Env):
    """Runs the `restore` CLI command,.

    Args:
        env: CLI environment.

    """
    dump_id_or_tag = env.get_arg('<dump>')
    target = env.get_arg('<target>')
    bucket = env.get_arg('-b')

    stash = repo.StashRepo.load(bucket)
    dump = stash.get_dump(dump_id_or_tag)
    if not dump:
        return env.die(f'❌ Dump not found: {dump_id_or_tag}')

    env.info(f'🥤 Restoring dump...')

    with storage.stream_storage_url(dump.storage_url) as stream:
        writer.write_dump(target, stream)

    env.ok(f'✅ Dump restored: id: {dump.dump_id}')


def _add_dump(storage_url: str, stash: models.Stash, tags: List[str] = None):
    """Adds a new dump (with optional tag) to the stash.

    Args:
        storage_url: URL to the dump file.
        stash: The stash to add the dump to.
        tags: Optional tags.

    Returns:
        Dump: The newly added dump.

    """
    dump = stash.add_dump(storage_url)
    return stash.tag_dump(dump, tags or [])


def _safely_update_stash(
    update_fn: Callable, stash: models.Stash, tries: int = 5
) -> Any:
    """Executes the update operation on the stash in a conflict-free manner, by re-loading
    and re-trying the stash update if a version conflict occures.

    Args:
        update_fn: The function which updates the stash.
        stash: The stash to update.
        tries: Optional number of tries, defaults to 5.

    Returns:
        Any: The `update_fn` return value.

    """
    remaining = tries

    while remaining > 0:
        remaining -= 1
        try:
            result = update_fn(stash)
            repo.StashRepo.save(stash)
            return result
        except repo.VersionConflict:
            stash = repo.StashRepo.load(stash.bucket)
