#!/usr/bin/env python3
"""Regenerate tests/swift-vectors.json from the actual Swift miner core and
coordinator, unchanged, with a deterministic injected worker and node.

Usage: swift-fixtures.py <lattice-node checkout> [revision]

The checkout must be clean and at the revision (default: the one pinned in
tests/swift-evidence.json). The oracle target is added to its Package.swift
for the run and removed afterwards; use a disposable worktree.
"""
import hashlib, json, pathlib, shutil, subprocess, sys

root = pathlib.Path(__file__).resolve().parents[1]
ref = pathlib.Path(sys.argv[1]).resolve()
evidence = root / 'tests/swift-evidence.json'
pin = sys.argv[2] if len(sys.argv) > 2 else json.loads(evidence.read_text())['node_revision']
head = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ref, text=True).strip()
assert head == pin, f'{ref} is at {head}, not {pin}'
assert not subprocess.check_output(['git', 'status', '--porcelain', '--untracked-files=no'], cwd=ref), 'checkout not clean'

sources = {}
for target, files in [('LatticeMinerCore', ['MinerLoopLogic.swift', 'ProofOfWork.swift']),
                      ('LatticeMiningCoordinator', ['MiningCoordinator.swift'])]:
    for name in files:
        path = ref / 'Sources' / target / name
        sources[str(path.relative_to(ref))] = hashlib.sha256(path.read_bytes()).hexdigest()

package = ref / 'Package.swift'
original = package.read_text()
oracle = ref / 'Sources/CoordinatorOracle'
try:
    oracle.mkdir()
    shutil.copy(root / 'tests/coordinator-oracle.swift', oracle / 'Oracle.swift')
    package.write_text(original.replace('    targets: [\n', '''    targets: [
        .executableTarget(name: "CoordinatorOracle", dependencies: ["LatticeMinerCore", "LatticeMiningCoordinator", .product(name: "Lattice", package: "lattice"), .product(name: "UInt256", package: "UInt256")]),
''', 1))
    raw = subprocess.check_output(['xcrun', 'swift', 'run', '--package-path', str(ref), 'CoordinatorOracle'])
finally:
    package.write_text(original)
    shutil.rmtree(oracle, ignore_errors=True)

fixture = json.loads(raw)
vectors = root / 'tests/swift-vectors.json'
vectors.write_text(json.dumps(fixture, indent=2, sort_keys=True) + '\n')
resolved = ref / 'Package.resolved'
report = {
    'node_revision': pin,
    'lattice': next(p['state'] for p in json.loads(resolved.read_text())['pins'] if p['identity'] == 'lattice'),
    'source_sha256': sources,
    'package_resolved_sha256': hashlib.sha256(resolved.read_bytes()).hexdigest(),
    'fixture_sha256': hashlib.sha256(vectors.read_bytes()).hexdigest(),
    'scope': 'Actual unchanged miner core/coordinator with deterministic injected worker/node; no live HTTP/mining.',
}
evidence.write_text(json.dumps(report, indent=2) + '\n')
print({k: len(v) for k, v in fixture.items()})
