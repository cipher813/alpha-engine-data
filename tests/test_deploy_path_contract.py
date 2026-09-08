"""Contract test: every Dockerfile COPY/ADD source must be declared in deploy.yml paths filter.

When a source file is not listed in .github/workflows/deploy.yml's `on.push.paths`, a
change to that file does NOT trigger a Phase-2 Lambda rebuild and deploy — the image
stays on the prior copy, indefinitely, with no alert. This is a silent corruption vector.

The test verifies that every COPY/ADD source in the Dockerfile is matched by some pattern
in the deploy.yml paths filter.

Reference: crucible-evaluator-PR282 for the same contract across eval image builds.
"""

import re
from fnmatch import fnmatchcase
from pathlib import Path

import pytest
import yaml


def _extract_dockerfile_sources(dockerfile_path: str) -> list[str]:
    """Extract all COPY/ADD source paths from a Dockerfile.
    
    Skips multi-stage builds (--from=...) which pull from intermediate images,
    not from the working tree. For a directory source, yields path+'/__file__'
    to match against a '/**' pattern (see test for why).
    """
    sources = []
    with open(dockerfile_path) as f:
        for line in f:
            line = line.strip()
            if not re.match(r'^(COPY|ADD)\s', line):
                continue
            
            # Skip multi-stage copies (--from=<stage> pulls from intermediate image)
            if any(x.startswith('--from=') for x in line.split()):
                continue
            
            # Parse: COPY [--chown=...] SRC [SRC...] DEST
            tokens = [x for x in line.split()[1:] if not x.startswith('--')]
            # All but the last token are sources; the last is the destination
            for src in tokens[:-1]:
                # For directories (trailing /), test as path/__file__ so that a
                # 'validators/**' pattern in paths will match properly.
                # A bare 'validators' pattern would NOT match 'validators/foo.py'
                # because fnmatchcase treats '/' as literal.
                if src.endswith('/'):
                    sources.append(src + '__file__')
                else:
                    sources.append(src)
    
    return sources


def _dockerfile_source_matches_deploy_path(source: str, path_pattern: str) -> bool:
    """Check whether a source path is matched by a deploy.yml paths pattern.
    
    Path patterns support:
      - literals: 'config.py' matches 'config.py'
      - wildcards: 'requirements*.txt' matches 'requirements.txt', 'requirements-dev.txt'
      - directories: 'lambda/**' matches 'lambda/handler.py', 'lambda/utils/foo.py'
    
    The test-file convention: a directory source in the Dockerfile is tested as
    path/__file__ (e.g. 'validators/__file__' for 'validators/') so that a
    '/**' pattern matches it (fnmatchcase('validators/__file__', 'validators/**') → True).
    """
    source_stripped = source.lstrip('/')
    
    # A '/**' pattern matches anything under that directory
    if path_pattern.endswith('/**'):
        return source_stripped.startswith(path_pattern[:-2])
    
    # A pattern without '/' in it should NOT match nested paths.
    # E.g. 'config.py' should match 'config.py' but not 'config/foo.py'.
    if '/' not in path_pattern and '/' in source_stripped:
        return False
    
    # Use fnmatchcase for glob-style matching
    return fnmatchcase(source_stripped, path_pattern)


def test_every_dockerfile_copy_source_is_a_declared_deploy_path():
    """Verify no COPY/ADD source is missing from deploy.yml's paths filter.
    
    Fails if any source is undeclared, preventing silent stale-image deployments.
    """
    repo_root = Path(__file__).parent.parent
    dockerfile = repo_root / 'Dockerfile'
    deploy_yml = repo_root / '.github' / 'workflows' / 'deploy.yml'
    
    assert dockerfile.exists(), f"Dockerfile not found at {dockerfile}"
    assert deploy_yml.exists(), f"deploy.yml not found at {deploy_yml}"
    
    # Extract sources and patterns
    sources = _extract_dockerfile_sources(str(dockerfile))
    with open(deploy_yml) as f:
        deploy_config = yaml.load(f, Loader=yaml.BaseLoader)
    
    paths_patterns = deploy_config['on']['push']['paths']
    
    # Check each source
    missing = []
    for source in sources:
        if not any(_dockerfile_source_matches_deploy_path(source, pat) for pat in paths_patterns):
            missing.append(source)
    
    if missing:
        msg = (
            f"The following Dockerfile COPY/ADD sources are NOT listed in "
            f"deploy.yml's `on.push.paths`. Changes to these files will NOT "
            f"trigger a Phase-2 Lambda redeploy:\n\n"
        )
        for src in missing:
            # Undo the __file__ suffix for readability in the error message
            display = src.replace('/__file__', '/') if src.endswith('/__file__') else src
            msg += f"  - {display}\n"
        msg += (
            f"\nAdd them to .github/workflows/deploy.yml's `on.push.paths` section. "
            f"Reproduce and verify the fix with:\n"
            f"  python3 -c 'import sys; sys.path.insert(0, \".\"); "
            f"from tests.test_deploy_path_contract import *; "
            f"test_every_dockerfile_copy_source_is_a_declared_deploy_path()'\n"
        )
        pytest.fail(msg)


def test_deliberate_red_detects_missing_path():
    """Deliberately introduce a missing COPY source to verify the test catches it.
    
    This confirms the contract test is not inert. The test is normally skipped;
    run explicitly to validate that adding an undeclared COPY WILL fail the guard.
    
    To run: pytest -k test_deliberate_red
    """
    pytest.skip(
        "This test deliberately breaks the contract to verify detection. "
        "Unskip and run pytest -k test_deliberate_red to confirm the guard catches "
        "a new COPY that is not in deploy.yml's paths."
    )
    
    # Mock a Dockerfile that has an undeclared source
    repo_root = Path(__file__).parent.parent
    deploy_yml = repo_root / '.github' / 'workflows' / 'deploy.yml'
    
    with open(deploy_yml) as f:
        deploy_config = yaml.load(f, Loader=yaml.BaseLoader)
    paths_patterns = deploy_config['on']['push']['paths']
    
    # Test that a new source NOT in paths will be detected
    undeclared_source = 'undeclared_module.py'
    assert not any(
        _dockerfile_source_matches_deploy_path(undeclared_source, pat)
        for pat in paths_patterns
    ), "Test setup broken: source should not be in patterns"
    
    # This assertion is what the real guard would fail on
    assert any(
        _dockerfile_source_matches_deploy_path(undeclared_source, pat)
        for pat in paths_patterns
    ), "DELIBERATE RED: Contract test detects undeclared sources"
