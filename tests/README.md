# Python tests

Use Python **3.14** for all Python tests. `.python-version` records the version
for local environment managers, and GitHub Actions selects it explicitly.
The Moto launcher checks its virtual environment's version before running tests.

```sh
python3.14 -m venv .venv-moto
.venv-moto/bin/python -m pip install -r tests/moto/requirements.txt -r tests/durable/requirements.txt
.venv-moto/bin/python -m unittest discover -s tests -p 'test_*.py'
.venv-moto/bin/python -m pytest -q tests/durable
```

If `.venv-moto` was created with an older interpreter, move it aside before
creating the replacement. Smoke scripts also use this environment, for example
`.venv-moto/bin/python tests/smoke-lambda.py` after building `roto-server`.

See [durable/README.md](durable/README.md) for the durable SDK fixture's scope.
