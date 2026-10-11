# roto

An MIT-licensed AWS simulator written in Rust, with persistent SQLite state and
S3 objects stored as real files. One binary serves AWS wire protocols on one port.

```sh
uv tool install roto-aws
roto-server --port 5070
```

**[Read the roto book](https://vivainio.github.io/roto/)** for
[getting started](https://vivainio.github.io/roto/getting-started/),
[service coverage](https://vivainio.github.io/roto/services/),
[performance results](https://vivainio.github.io/roto/benchmarks/), and
[development](https://vivainio.github.io/roto/development/).

The hand-tuned Rust core services are where we aim to offer a first-class
experience for typical AWS applications. Optional Lua services provide basic
administration and configuration APIs and require a separate bundle download;
see the book's [installation guide](https://vivainio.github.io/roto/getting-started/#add-optional-lua-services).

Under active development. See [STATUS.md](STATUS.md) for detailed coverage,
[PLAN.md](PLAN.md) for the roadmap, and [LICENSE](LICENSE) for the MIT license.
