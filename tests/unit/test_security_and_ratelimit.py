import pytest
from pydantic import SecretStr

from app.api.ratelimit import RateLimiter
from app.core.errors import (
    ForbiddenError,
    InvalidRequestError,
    RateLimitedError,
    UnauthorizedError,
)
from app.core.security import Caller, authenticate, parse_identity
from app.core.settings import ApiSettings, Settings

API = ApiSettings(
    service_tokens={"java-search": SecretStr("token-java"), "other": SecretStr("token-other")},
    admin_tokens={"ops": SecretStr("token-admin")},
    identity_services=["java-search"],
    max_groups=3,
)


def _h(**headers: str) -> dict[str, str]:
    return {k.replace("_", "-").lower(): v for k, v in headers.items()}


# --- service authentication -----------------------------------------------------------------


def test_a_valid_token_gives_the_service_name() -> None:
    assert authenticate(_h(authorization="Bearer token-java"), API) == Caller("java-search")
    assert authenticate(_h(authorization="bearer token-other"), API).service == "other"


@pytest.mark.parametrize(
    "headers",
    [
        {},
        {"authorization": ""},
        {"authorization": "Bearer"},
        {"authorization": "Bearer   "},
        {"authorization": "Basic token-java"},
        {"authorization": "Bearer wrong"},
        {"authorization": "Bearer token-java-and-more"},
        {"authorization": "Bearer token-admin"},  # an admin token is not a service token
        {"x-user-id": "u1"},
    ],
)
def test_missing_or_wrong_tokens_are_refused(headers: dict[str, str]) -> None:
    with pytest.raises(UnauthorizedError):
        authenticate(headers, API)


def test_with_no_tokens_configured_everything_is_refused() -> None:
    with pytest.raises(UnauthorizedError):
        authenticate(_h(authorization="Bearer anything"), ApiSettings())
    with pytest.raises(UnauthorizedError):
        authenticate({}, ApiSettings())


def test_admin_tokens_are_separate() -> None:
    assert authenticate(_h(authorization="Bearer token-admin"), API, admin=True) == Caller(
        "ops", True
    )
    with pytest.raises(UnauthorizedError):
        authenticate(_h(authorization="Bearer token-java"), API, admin=True)


def test_auth_can_be_switched_off_for_development_but_not_in_prod(settings: Settings) -> None:
    assert authenticate({}, ApiSettings(auth_disabled=True)).service == "dev"
    with pytest.raises(ValueError, match="auth_disabled"):
        Settings.model_validate(
            {**settings.model_dump(), "env": "prod", "api": {"auth_disabled": True}}
        )


# --- end-user identity ----------------------------------------------------------------------


def test_a_trusted_service_may_send_the_user() -> None:
    caller = Caller("java-search")
    identity = parse_identity(_h(x_user_id="alice", x_user_groups="g1, g2,g1"), caller, API)
    assert (identity.user_id, identity.groups, identity.service) == (
        "alice",
        ("g1", "g2"),
        "java-search",
    )


def test_an_untrusted_service_cannot_send_user_identity() -> None:
    with pytest.raises(ForbiddenError):
        parse_identity(_h(x_user_id="alice"), Caller("other"), API)


def test_a_request_without_a_user_is_refused() -> None:
    for headers in ({}, {"x-user-id": ""}, {"x-user-id": "  "}):
        with pytest.raises(InvalidRequestError):
            parse_identity(headers, Caller("java-search"), API)


@pytest.mark.parametrize("user", ["al*ice", "a\nb", "x" * 300, "ali;ce", "al\x00ice", "a'b"])
def test_odd_user_ids_are_refused(user: str) -> None:
    with pytest.raises(InvalidRequestError):
        parse_identity({"x-user-id": user}, Caller("java-search"), API)


def test_odd_group_names_are_refused() -> None:
    with pytest.raises(InvalidRequestError):
        parse_identity({"x-user-id": "u", "x-user-groups": "g1,g*"}, Caller("java-search"), API)


def test_too_many_groups_are_refused() -> None:
    with pytest.raises(InvalidRequestError):
        parse_identity({"x-user-id": "u", "x-user-groups": "a,b,c,d"}, Caller("java-search"), API)


def test_no_groups_is_fine() -> None:
    identity = parse_identity({"x-user-id": "u"}, Caller("java-search"), API)
    assert identity.groups == ()


def test_identity_is_not_trusted_when_auth_is_off_only_in_development() -> None:
    dev = ApiSettings(auth_disabled=True)
    assert parse_identity({"x-user-id": "u"}, Caller("dev"), dev).user_id == "u"


# --- rate limits ----------------------------------------------------------------------------


class _Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def test_requests_over_the_limit_are_refused_with_a_wait_time() -> None:
    clock = _Clock()
    limiter = RateLimiter(3, 100, clock=clock)
    for _ in range(3):
        limiter.check("user", "alice")
    clock.now = 10
    with pytest.raises(RateLimitedError) as error:
        limiter.check("user", "alice")
    assert error.value.retry_after_s == 51
    assert error.value.http_status == 429


def test_the_window_resets_after_a_minute() -> None:
    clock = _Clock()
    limiter = RateLimiter(1, 100, clock=clock)
    limiter.check("user", "alice")
    with pytest.raises(RateLimitedError):
        limiter.check("user", "alice")
    clock.now = 60
    limiter.check("user", "alice")


def test_users_and_services_are_counted_separately() -> None:
    limiter = RateLimiter(1, 1, clock=_Clock())
    limiter.check("user", "alice")
    limiter.check("user", "bob")
    limiter.check("service", "alice")  # the same name as a service is another counter
    with pytest.raises(RateLimitedError):
        limiter.check("user", "alice")


def test_a_limit_of_zero_switches_it_off() -> None:
    limiter = RateLimiter(0, 0, clock=_Clock())
    for _ in range(1000):
        limiter.check("user", "alice")


def test_old_windows_are_dropped_when_the_table_is_full() -> None:
    clock = _Clock()
    limiter = RateLimiter(5, 5, clock=clock, max_keys=10)
    for i in range(10):
        limiter.check("user", f"u{i}")
    clock.now = 120
    limiter.check("user", "new")
    assert len(limiter._windows) == 1
