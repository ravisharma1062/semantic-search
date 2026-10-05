"""Service authentication and end-user identity (HLD sections 8 and 9).

- Every request carries a service token (``Authorization: Bearer ...``). Tokens come from the secret
  store, one per calling service. Comparison is constant time. With no token configured, every
  request is
  refused (fail closed), unless ``api.auth_disabled`` is set, which prod refuses.
- The end user (``X-User-Id`` and ``X-User-Groups``) is read only from a service listed in
  ``api.identity_services``. Anyone else who sends these headers is refused, never trusted.
- Admin calls use separate admin tokens.
"""

import hmac
import re
from collections.abc import Mapping
from dataclasses import dataclass

from app.core.errors import ForbiddenError, InvalidRequestError, UnauthorizedError
from app.core.settings import ApiSettings

USER_HEADER = "X-User-Id"
GROUPS_HEADER = "X-User-Groups"
_ID = re.compile(r"^[A-Za-z0-9._@:+\-/ ]{1,256}$")


@dataclass(frozen=True)
class Caller:
    """An authenticated calling service."""

    service: str
    is_admin: bool = False


@dataclass(frozen=True)
class Identity:
    """The end user a request is for. Access filters are built from this and from nothing else."""

    user_id: str
    groups: tuple[str, ...]
    service: str


def _bearer(headers: Mapping[str, str]) -> str | None:
    value = headers.get("authorization", "")
    scheme, _, token = value.partition(" ")
    if scheme.lower() != "bearer" or not token.strip():
        return None
    return token.strip()


def _match(token: str, tokens: Mapping[str, object]) -> str | None:
    """The name whose token matches. Every token is compared, so timing does not tell which."""
    found: str | None = None
    for name, secret in tokens.items():
        value = secret.get_secret_value() if hasattr(secret, "get_secret_value") else str(secret)
        if hmac.compare_digest(token.encode(), value.encode()):
            found = name
    return found


def authenticate(headers: Mapping[str, str], api: ApiSettings, *, admin: bool = False) -> Caller:
    """Check the service token. ``headers`` must have lower-case names."""
    if api.auth_disabled:
        return Caller(service="dev", is_admin=True)
    token = _bearer(headers)
    if token is None:
        raise UnauthorizedError("Missing or invalid service token")
    tokens = api.admin_tokens if admin else api.service_tokens
    name = _match(token, tokens)
    if name is None:
        raise UnauthorizedError("Missing or invalid service token")
    return Caller(service=name, is_admin=admin)


def _check_id(value: str, what: str) -> str:
    value = value.strip()
    if not _ID.match(value):
        raise InvalidRequestError(f"Invalid {what}")
    return value


def parse_identity(headers: Mapping[str, str], caller: Caller, api: ApiSettings) -> Identity:
    """The end user from the identity headers. Only trusted services may send them."""
    if not api.auth_disabled and caller.service not in api.identity_services:
        raise ForbiddenError("This service may not send user identity")
    user = headers.get(USER_HEADER.lower())
    if user is None or not user.strip():
        raise InvalidRequestError("Missing user identity")
    user_id = _check_id(user, "user id")
    groups: list[str] = []
    for raw in headers.get(GROUPS_HEADER.lower(), "").split(","):
        if raw.strip():
            groups.append(_check_id(raw, "group"))
    unique = tuple(dict.fromkeys(groups))
    if len(unique) > api.max_groups:
        raise InvalidRequestError("Too many groups")
    return Identity(user_id=user_id, groups=unique, service=caller.service)
