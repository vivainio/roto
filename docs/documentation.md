# Documentation

This book uses [Zensical](https://zensical.org/) with Markdown sources under
`docs/` and navigation and theme settings in `zensical.toml`.

## Preview and build

Use an isolated Python environment:

```sh
python3 -m venv .venv-docs
.venv-docs/bin/python -m pip install -r requirements-docs.txt
.venv-docs/bin/zensical serve
```

For the same validation as CI:

```sh
.venv-docs/bin/zensical build --clean --strict
```

The output is `site/`; generated output and caches are ignored by Git. Add new
chapters to `nav` in `zensical.toml`. Keep coverage claims aligned with source
code and `STATUS.md`.

## GitHub Pages

The [Documentation workflow](https://github.com/vivainio/roto/blob/main/.github/workflows/docs.yml)
builds pull requests and publishes pushes to `main`. It also supports a manual
run from the Actions tab on `main`. Only the deployment job receives Pages write
and identity-token permissions.

In the repository's **Settings → Pages → Build and deployment**, set **Source**
to **GitHub Actions**. After the workflow deploys, the book is available at
[https://vivainio.github.io/roto/](https://vivainio.github.io/roto/).

The workflow follows Zensical's
[GitHub Pages publishing instructions](https://zensical.org/docs/publish-your-site/),
with a separate build job so pull requests validate without deploying.
