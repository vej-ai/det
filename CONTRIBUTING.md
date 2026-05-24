# Contributing to det

Thanks for your interest in det. This is a short stub — it will grow as the
project does. The governing principle is in the
[design handbook](./docs/00-vision-and-naming.md): **keep it as simple as
possible.**

## Dev setup

det targets Python 3.11+. Clone the repo, then create a virtual environment
and install the package in editable mode with the `dev` extras:

```sh
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

## Run the tests

```sh
pytest -q
```

## Run the linter and type checker

```sh
ruff check .
mypy det
```

Please make sure `pytest`, `ruff check`, and `mypy` all pass before opening a
pull request.

## Spec precedence: code is the source of truth

The [design handbook](./docs/) (`docs/00`–`docs/11`) was written before the
implementation and remains the canonical *intent*. But once a contract is
implemented in code, **the code is the source of truth** — if the handbook and
the code diverge, fix the handbook to match the code, not the other way around.
This avoids re-litigating handbook ambiguities at every stage. Notable already:
`det/types.py::StateRecord` defines the canonical `_det_state` schema
(docs/03 §3.5 and docs/05 §5.1 follow it).
