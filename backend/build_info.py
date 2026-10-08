"""Identify shipped code without reading runtime data, secrets or the Docker socket."""
from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path

APP_VERSION = '0.2.0'


def _fingerprint(root: Path) -> str:
    paths = set()
    for folder in ('backend', 'frontend'):
        for path in (root / folder).rglob('*'):
            if not path.is_file() or path.is_symlink():
                continue
            relative = path.relative_to(root)
            if '__pycache__' in relative.parts or 'node_modules' in relative.parts or path.name.startswith('test_'):
                continue
            if folder == 'frontend' or path.suffix == '.py' or path.name.endswith('.seed.json') or path.name == 'requirements.txt':
                paths.add(path)
    paths.update(root / name for name in ('Dockerfile', 'docker-compose.yml') if (root / name).is_file())
    digest = hashlib.sha256()
    for path in sorted(paths, key=lambda item: item.relative_to(root).as_posix()):
        digest.update(path.relative_to(root).as_posix().encode() + b'\0')
        digest.update(hashlib.sha256(path.read_bytes()).digest())
    return digest.hexdigest()


def create_build_info(root: Path, *, revision: str = '') -> dict:
    return {'version': APP_VERSION, 'source_id': _fingerprint(root),
            'revision': revision.lower() if re.fullmatch(r'[a-fA-F0-9]{7,64}', revision or '') else None,
            'built_at': datetime.now(timezone.utc).isoformat(), 'source': 'image-build'}


def read_build_info(root: Path) -> dict:
    try:
        fingerprint = _fingerprint(root)
    except OSError:
        fingerprint = None
    fallback = {'version': APP_VERSION, 'source_id': fingerprint, 'revision': None,
                'built_at': None, 'source': 'source-files' if fingerprint else 'unavailable'}
    try:
        path = root / 'build-info.json'
        if path.stat().st_size > 4096:
            return fallback
        saved = json.loads(path.read_text(encoding='utf-8'))
        if not isinstance(saved, dict) or not fingerprint or saved.get('source_id') != fingerprint or saved.get('version') != APP_VERSION:
            return fallback
        date = datetime.fromisoformat(saved['built_at'])
        if date.tzinfo is None:
            return fallback
        revision = saved.get('revision')
        if revision is not None and (not isinstance(revision, str) or not re.fullmatch(r'[a-f0-9]{7,64}', revision)):
            return fallback
        return {**fallback, 'revision': revision, 'built_at': date.astimezone(timezone.utc).isoformat(), 'source': 'image-build'}
    except (OSError, ValueError, KeyError, TypeError):
        return fallback


@lru_cache(maxsize=2)
def get_build_info(root: Path) -> dict:
    return read_build_info(root)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--write', type=Path, required=True)
    parser.add_argument('--revision', default='')
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    args.write.write_text(json.dumps(create_build_info(root, revision=args.revision), indent=2), encoding='utf-8')
