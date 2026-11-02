#!/usr/bin/env python3
# Offline harness for the fork's immutable ci-test-* tag guard in
# .github/workflows/docker-image.yml (merge job, "Create manifest lists").
# Extracts the actual workflow shell text and drives it with a stub
# `docker` whose exit status and stderr are scripted per case.
#
# Usage: python3 .github/workflows/tests/aisix-tag-guard-test.py
import os
from pathlib import Path
import subprocess
import tempfile

root = Path(__file__).resolve().parents[3]
workflow = (root / '.github/workflows/docker-image.yml').read_text()
marker = '      - name: Create manifest lists\n'
assert workflow.count(marker) == 1
block = workflow.split(marker, 1)[1].split('\n      - name:', 1)[0]
body = block.split('        run: |\n', 1)[1]
# Strip exactly the 10 leading spaces of the YAML block scalar.
script = '\n'.join(line[10:] if line.startswith('          ') else line
                   for line in body.splitlines())
# The workflow reads the tag set from the artifact; inject it instead.
script = script.replace('TAGS="$(cat "$RUNNER_TEMP/tagset/tags.txt")"',
                        'TAGS="$INJECTED_TAGS"', 1)
lines = script.splitlines()
done_idx = next(i for i, l in enumerate(lines) if l == 'done')
guard_only = '\n'.join(lines[:done_idx + 1])
assert 'docker buildx imagetools inspect' in guard_only

image = 'ghcr.io/xz-dev/aisix'
tag = 'ci-test-06e8075'
missing = f'ERROR: {image}:{tag}: manifest unknown'
cases = [
    # (name, stub exit, stub stderr, guard must pass, tag under test)
    ('existing scratch tag',       0, '', False, f'{image}:{tag}'),
    ('confirmed manifest unknown', 1, missing, True, f'{image}:{tag}'),
    ('transport failure',         42, 'ERROR: failed to resolve reference: registry connection timed out', False, f'{image}:{tag}'),
    ('auth failure',               1, 'ERROR: unauthorized: authentication required', False, f'{image}:{tag}'),
    ('unrelated not-found error',  1, 'ERROR: open registry config: file not found', False, f'{image}:{tag}'),
    ('mixed unknown and auth',     1, missing + '\nERROR: unauthorized: authentication required', False, f'{image}:{tag}'),
    ('mixed unknown and HTTP503',  1, missing + '\nERROR: unexpected status 503 Service Unavailable', False, f'{image}:{tag}'),
    ('mixed unknown and TLS',     42, missing + '\nERROR: x509: certificate signed by unknown authority', False, f'{image}:{tag}'),
    ('wrong reference',           1, 'ERROR: ghcr.io/xz-dev/unrelated:ci-test-06e8075: manifest unknown', False, f'{image}:{tag}'),
    ('missing with abnormal exit',42, missing, False, f'{image}:{tag}'),
    ('Buildx exact not found',     1, f'ERROR: {image}:{tag}: not found', True, f'{image}:{tag}'),
    ('not found abnormal exit',   42, f'ERROR: {image}:{tag}: not found', False, f'{image}:{tag}'),
    ('manifest blob missing',      1, f'ERROR: {image}:{tag}: manifest blob unknown', False, f'{image}:{tag}'),
    ('empty unknown failure',      1, '', False, f'{image}:{tag}'),
    ('non-scratch tag skipped',    0, '', True, f'{image}:sha-06e8075'),
]

with tempfile.TemporaryDirectory(prefix='aisix-tag-guard-', dir=os.environ.get('TMPDIR', '/var/tmp')) as d:
    docker = Path(d) / 'docker'
    docker.write_text('#!/bin/sh\nprintf \'%s\\n\' "$GUARD_MESSAGE" >&2\nexit "$GUARD_STATUS"\n')
    docker.chmod(0o700)
    for name, status, message, allowed, tag_ref in cases:
        env = dict(os.environ, PATH=d + os.pathsep + os.environ['PATH'],
                   INJECTED_TAGS=tag_ref, GUARD_STATUS=str(status), GUARD_MESSAGE=message)
        result = subprocess.run(['bash', '--noprofile', '--norc', '-e', '-o', 'pipefail', '-c', guard_only],
                                env=env, capture_output=True, text=True, timeout=10)
        ok = (result.returncode == 0) == allowed
        if status not in (0, 1) and not allowed:
            # Unexpected failures must retain their original status.
            ok = ok and result.returncode == status
        assert ok, f'{name}: unexpected guard exit {result.returncode}: {result.stderr[-200:]}'
print(f'immutable ci-test tag guard: {len(cases)} cases passed')
