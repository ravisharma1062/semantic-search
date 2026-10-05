"""The access filter (rule 1). One function builds it, nobody else does.

A chunk is visible when the user ID is in ``acl_users`` or one of the user's groups is in
``acl_groups``. The filter goes into every query, also inside the kNN part, so restricted chunks
never enter a candidate list, the reranker or an LLM prompt (HLD section 9, "pre-filter, not
post-filter").
"""

import hashlib
import json
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from app.core.errors import ForbiddenError
from app.core.security import Identity


class AclFilter(BaseModel):
    """The rights of one user, as an Elasticsearch filter."""

    model_config = ConfigDict(frozen=True)

    user_id: str = Field(min_length=1)
    groups: tuple[str, ...] = ()

    @classmethod
    def from_identity(cls, identity: Identity) -> "AclFilter":
        """The filter for an identity. A missing user means no access at all."""
        if not identity.user_id.strip():
            raise ForbiddenError("No user identity")
        return cls(user_id=identity.user_id, groups=tuple(sorted(set(identity.groups))))

    def to_query(self) -> dict[str, Any]:
        """The filter clause. The user ID alone, or the user ID or any of the groups."""
        should: list[dict[str, Any]] = [{"term": {"acl_users": self.user_id}}]
        if self.groups:
            should.append({"terms": {"acl_groups": list(self.groups)}})
        return {"bool": {"should": should, "minimum_should_match": 1}}

    def scope_key(self) -> str:
        """A stable key for this set of rights. Cached results are shared only inside one scope."""
        canonical = json.dumps([self.user_id, list(self.groups)], separators=(",", ":"))
        return hashlib.sha256(canonical.encode()).hexdigest()[:32]
