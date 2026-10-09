# roto

An AWS simulator written in Rust: a from-scratch rewrite of [moto](https://github.com/getmoto/moto)'s server mode.
One static binary, AWS wire-protocol compatible, persistent by default (SQLite + real-filesystem S3).

**Status:** early (Phase 0). See [PLAN.md](PLAN.md).

```sh
cargo run -p roto-server -- --port 5000        # persistent: ./roto-data ; add --ephemeral for in-memory
aws --endpoint-url http://localhost:5000 sts get-caller-identity
scripts/run-moto-tests.sh test_sts              # moto's own tests, run against roto
```

## License

MIT. See [LICENSE](LICENSE).
