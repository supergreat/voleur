import dataclasses
import uuid
from datetime import datetime
from typing import List, Optional


@dataclasses.dataclass
class Dump:
    # A unique dump id.
    dump_id: str

    # When was this dump created.
    timestamp: str

    # URL to the dump file.
    storage_url: str


@dataclasses.dataclass
class Stash:
    # The stash name.
    name: str

    # Mapping of tag -> dump_id.
    tags: dict = dataclasses.field(default_factory=dict)

    # List of dumps in the stash.
    dumps: List[Dump] = dataclasses.field(default_factory=list)

    def get_dump(self, id_or_tag: str) -> Optional[Dump]:
        """Gets a dump by id or tag.

        Args:
            id_or_tag: Some identifier that can be dump id or tag.

        Returns:
            Optional[Dump]: Dump or None if not found.

        """
        dump_id = self.tags.get(id_or_tag, id_or_tag)
        hits = [d for d in self.dumps if d.dump_id == dump_id]
        return hits[0] if hits else None

    def add_dump(self, storage_url: str) -> Dump:
        """Adds a new dump.

        Args:
            storage_url: URL to dump file.

        Returns:
            Dump

        """
        dump = Dump(
            dump_id=uuid.uuid4().hex[:8],
            timestamp=datetime.utcnow().isoformat(),
            storage_url=storage_url,
        )
        self.dumps.append(dump)
        return dump

    def tag_dump(self, dump: Dump, tags: List[str]) -> Dump:
        """Tags a dump with one or more tags. This is an additive method i.e it will add
        new tags and override existing ones, but will not clear previously applied tags.

        Args:
            dump: The dump to tag.
            tags: List of tags.

        Returns:
            Dump

        """
        for tag in tags:
            self.tags[tag] = dump.dump_id
        return dump
