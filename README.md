# roto

An AWS simulator written in Rust: a from-scratch rewrite of [moto](https://github.com/getmoto/moto)'s server mode.
One static binary, AWS wire-protocol compatible, persistent by default (SQLite + real-filesystem S3).

**Status:** under active development. See [STATUS.md](STATUS.md) for current coverage and [PLAN.md](PLAN.md) for the roadmap.

**Documentation:** [The roto book](https://vivainio.github.io/roto/) ([sources](docs/index.md)).

```sh
cargo run -p roto-server -- --port 5070        # persistent: ./roto-data ; add --ephemeral for in-memory
aws --endpoint-url http://localhost:5070 sts get-caller-identity
scripts/run-moto-tests.sh test_sts              # moto's own tests, run against roto
```

## Documentation

```sh
python3 -m venv .venv-docs
.venv-docs/bin/python -m pip install -r requirements-docs.txt
.venv-docs/bin/zensical serve
```

GitHub Actions builds the book on pull requests and deploys `main` to GitHub Pages.
Set the repository's Pages source to **GitHub Actions**. See
[the documentation guide](docs/documentation.md) for details.

## License

MIT. See [LICENSE](LICENSE).
