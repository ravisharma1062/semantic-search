"""The evaluation set: questions with the correct documents and pages (HLD section 10).

The set has a dev slice for tuning and a test slice that must never be used for tuning. Loading the
test slice needs an explicit ``allow_test_slice``, so it is a conscious act.
"""

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field, model_validator

from app.core.errors import InvalidRequestError


class Relevant(BaseModel):
    """A correct document, and optionally the pages that hold the answer."""

    doc_id: str
    pages: list[int] = Field(default_factory=list)


class EvalQuestion(BaseModel):
    """One question. ``as_user`` and ``groups`` say whose rights the search uses."""

    id: str
    question: str
    relevant: list[Relevant] = Field(default_factory=list)
    answerable: bool = True
    reference_answer: str | None = None
    as_user: str = "eval-user"
    groups: list[str] = Field(default_factory=list)
    slice: Literal["dev", "test"] = "dev"
    tags: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _check(self) -> "EvalQuestion":
        if self.answerable and not self.relevant:
            raise ValueError("an answerable question needs at least one correct document")
        if not self.answerable and self.relevant:
            raise ValueError("a question without an answer has no correct documents")
        return self


def load_questions(
    path: Path, *, slice_name: Literal["dev", "test"] = "dev", allow_test_slice: bool = False
) -> list[EvalQuestion]:
    """Questions of one slice from a JSON Lines file."""
    if slice_name == "test" and not allow_test_slice:
        raise InvalidRequestError("The test slice must not be used for tuning")
    questions: list[EvalQuestion] = []
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            question = EvalQuestion.model_validate_json(line)
        except ValueError as exc:
            raise InvalidRequestError(f"{path.name} line {number} is not a valid question") from exc
        if question.slice == slice_name:
            questions.append(question)
    ids = [q.id for q in questions]
    if len(ids) != len(set(ids)):
        raise InvalidRequestError("Question IDs must be unique")
    return questions
