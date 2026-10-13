# Storage

Persistent mode uses one SQLite database per service, with WAL and versioned
migrations. API calls use transactions. `--durable` selects SQLite's
`synchronous=FULL` setting. Ephemeral mode uses in-memory databases and temporary
S3 files.

## S3 files

Current object bodies mirror the bucket and key layout:

```text
roto-data/
  s3/
    <bucket>/<key path>
    .roto/
```

Non-current versions and in-flight multipart parts live under `.roto`. Writes
use a temporary file followed by an atomic rename. When a key is also a prefix,
such as `a` alongside `a/b`, the object at `a` is stored as `a/.roto-self`.
Unsafe or overly long key segments are escaped.

SQLite remains the source of truth for metadata and listings. You can browse or
copy the file tree, but copying new files into a bucket directory does not register
new objects. Upload fixtures through the S3 API, or register large host files without
copying them using `roto.s3.link` (see [Lua API](lua-api.md#s3)).

A linked object stores `link:<absolute host path>` instead of a body path. Reads are
served from the host file; roto never writes, moves or deletes it, so deleting the key,
overwriting it, or deleting the bucket only affects roto's metadata. The size is captured
when the link is made, which only stats the file. The real MD5 ETag is computed lazily, the
first time a `HEAD`, `GET` or listing needs it, and is remembered in memory until the file's
size or modification time changes, so a large linked file costs nothing until it is read.
`HEAD` and listings report the size from link time: relink (or restart) after editing a
linked file. If the file disappears, reads fail with `InternalFailure` naming it.

## Current limits

The planned S3 rescan command, crash-recovery reconciliation, and additional
filesystem fsync controls are not implemented. Treat the on-disk format as under
development. See [Server reference](server.md) for storage options and the reset
endpoint.
