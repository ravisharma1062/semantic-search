"""Audit records for admin actions (HLD section 17, "Privacy and audit").

One structured log line per action, on the ``audit`` logger, with the actor (the admin service
name), the action, the outcome and IDs. Never text. The central log platform ships these lines to
the audit store. The request ID is added by the logging setup, so an audit line can be tied to the
request.
"""

from typing import Literal

import structlog

from app.observability.metrics import get_metrics

_audit = structlog.get_logger("audit")

Outcome = Literal["accepted", "rejected", "failed"]


def record(actor: str, action: str, outcome: Outcome, **ids: str | int | bool | None) -> None:
    """Write one audit line and count it. ``ids`` are IDs and flags only (``item_id``,
    ``job_id``, ``wave``)."""
    get_metrics().admin_actions.labels(action, outcome).inc()
    _audit.info("admin_action", audit=True, actor=actor, action=action, outcome=outcome, **ids)
