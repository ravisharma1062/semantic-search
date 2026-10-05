"""Checks and cleans the vectors that come back from a model server."""

import math
from typing import Any

from app.core.errors import NonRetryableError

_NORM_TOLERANCE = 1e-3


def clean_vectors(raw: Any, expected_count: int, dims: int) -> list[list[float]]:
    """Validate the server answer and return unit-length vectors.

    The index uses cosine similarity (HLD section 4), so vectors are normalized here when the
    server did not do it. A wrong count, wrong size or a NaN is a configuration or model error
    that retrying cannot fix.
    """
    if not isinstance(raw, list) or len(raw) != expected_count:
        raise NonRetryableError("Embedding server returned an unexpected number of vectors")
    vectors: list[list[float]] = []
    for item in raw:
        if not isinstance(item, list) or len(item) != dims:
            raise NonRetryableError("Embedding dimension does not match the configuration")
        try:
            vector = [float(x) for x in item]
        except (TypeError, ValueError) as exc:
            raise NonRetryableError("Embedding server returned a bad vector") from exc
        if not all(math.isfinite(x) for x in vector):
            raise NonRetryableError("Embedding server returned a bad vector")
        norm = math.sqrt(sum(x * x for x in vector))
        if norm == 0:
            raise NonRetryableError("Embedding server returned a zero vector")
        if abs(norm - 1.0) > _NORM_TOLERANCE:
            vector = [x / norm for x in vector]
        vectors.append(vector)
    return vectors
