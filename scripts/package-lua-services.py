#!/usr/bin/env python3
"""Build the optional Lua service ZIP, with manifest.json as the first entry."""
import argparse
import json
from pathlib import Path, PurePosixPath
import zipfile

ROOT = Path(__file__).resolve().parents[1]


def package(bundle, output):
    bundle = bundle.resolve()
    manifest = json.loads((bundle / 'manifest.json').read_text())
    if manifest.get('version') != 1 or not isinstance(manifest.get('services'), list):
        raise ValueError('Expected a version 1 root bundle manifest')
    paths = ['manifest.json']
    if 'common' in manifest:
        paths.append(manifest['common'])
    paths.extend(service['script'] for service in manifest['services'])
    paths.extend(service['model'] for service in manifest['services'] if 'model' in service)
    paths.extend(name for name in ['README.md', 'LICENSE-MiniStack', 'LICENSE'] if (bundle / name).is_file())
    files = []
    for name in dict.fromkeys(paths):
        if not isinstance(name, str) or not name or '\\' in name or ':' in name:
            raise ValueError(f'Invalid bundle path: {name!r}')
        path = PurePosixPath(name)
        if path.is_absolute() or any(part in ('', '.', '..') for part in name.split('/')):
            raise ValueError(f'Invalid bundle path: {name}')
        source = (bundle / name).resolve()
        if not source.is_relative_to(bundle):
            raise ValueError(f'Bundle file escapes its directory: {name}')
        data = source.read_bytes()
        if len(data) > 1024 * 1024:
            raise ValueError(f'Bundle file exceeds 1 MiB: {name}')
        files.append((name, data))
    output.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(output, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for name, data in files:
            entry = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            entry.compress_type = zipfile.ZIP_DEFLATED
            entry.external_attr = 0o100644 << 16
            archive.writestr(entry, data, compresslevel=9)
    print(f'{output}: {len(files)} files, manifest.json first')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bundle', type=Path, default=ROOT / 'services')
    parser.add_argument('--output', type=Path, default=ROOT / 'dist/roto-lua-services.zip')
    args = parser.parse_args()
    package(args.bundle, args.output)


if __name__ == '__main__':
    main()
