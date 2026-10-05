"""FastAPI dependencies: authentication, identity, rate limits, services."""

from typing import cast

from fastapi import Depends, Request

from app.api.ratelimit import RateLimiter
from app.core.security import Caller, Identity, authenticate, parse_identity
from app.core.settings import Settings
from app.services import Services


def get_settings_dep(request: Request) -> Settings:
    """The settings of this app."""
    return cast(Settings, request.app.state.settings)


def get_services(request: Request) -> Services:
    """The use cases."""
    return cast(Services, request.app.state.services)


def get_limiter(request: Request) -> RateLimiter:
    """The rate limiter of this pod."""
    return cast(RateLimiter, request.app.state.limiter)


def get_caller(
    request: Request,
    settings: Settings = Depends(get_settings_dep),
    limiter: RateLimiter = Depends(get_limiter),
) -> Caller:
    """The authenticated service. Rate limited per service."""
    caller = authenticate(request.headers, settings.api)
    limiter.check("service", caller.service)
    return caller


def get_admin(
    request: Request,
    settings: Settings = Depends(get_settings_dep),
    limiter: RateLimiter = Depends(get_limiter),
) -> Caller:
    """An authenticated admin. Admin tokens are separate from service tokens."""
    caller = authenticate(request.headers, settings.api, admin=True)
    limiter.check("service", f"admin:{caller.service}")
    return caller


def get_identity(
    request: Request,
    caller: Caller = Depends(get_caller),
    settings: Settings = Depends(get_settings_dep),
    limiter: RateLimiter = Depends(get_limiter),
) -> Identity:
    """The end user. Only trusted services may send it. Rate limited per user."""
    identity = parse_identity(request.headers, caller, settings.api)
    limiter.check("user", identity.user_id)
    return identity
