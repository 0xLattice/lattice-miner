#!/usr/bin/env python3
"""Mine real blocks with lattice-miner through a local lattice-node.

A disposable node with no peers hosts Nexus and three children:

    Nexus/C    easy (halfLife 10^6 blocks: its target stays near the maximum)
    Nexus/P    1 h blocks and halfLife 1: one doubling per block mined early
    Nexus/P/G  easy, under P

The miner reaches the node only through its remote operator listener, with
a token scoped to the mining routes, and is never restarted: every change
below is made to its token file or miner.json while it runs.

1. Scoped token (POST /mining/templates, POST /mining/work, GET /status),
   built-in CPU worker: GET /mining/events is refused, so the miner polls
   GET /status. Nexus is mined, then C, P and G are created and mined, each
   block paying the recipient miner.json names for its chain.
2. The token gains GET /mining/events: the miner follows the pushed digest.
3. P is hardened to 2^--hard-bits work. A chain hardens only as its blocks
   come, and an easy Nexus ends each round at its first hash, so minWork on
   Nexus is kept at P's work meanwhile: every round's hit then clears P too.
4. Hard parent: minWork is dropped and the external worker
   (lattice-miner-gpu, --worker-backend) takes over. For --observe seconds
   G, under a P 2^--hard-bits times harder, must keep producing valid blocks
   (the grind walks through the P carrier it misses), far outpacing P; G's
   longest gap is bounded.

Usage: node-integration.py --node-bin DIR [--miner BIN] [--worker BIN]
DIR holds lattice-node and lattice built from the lattice-node revision under
test.
"""
import argparse, base64, os, hashlib, json, pathlib, shutil, signal, subprocess, tempfile, time, urllib.request

ROOT = pathlib.Path(__file__).resolve().parents[2]
PORTS = {'listen': 18401, 'rpc': 18480, 'operator': 18490}
SCOPED = ['POST /mining/templates', 'POST /mining/work', 'GET /status']


def work(target_hex):
    return (1 << 256) // (int(target_hex, 16) + 1)


class Node:
    def __init__(self, bins, root):
        self.bins, self.root = bins, root

    def ctl(self, *args):
        return subprocess.run([str(self.bins / 'lattice'), *args, '--root', str(self.root)],
                              check=True, capture_output=True, text=True).stdout

    def get(self, route, chain='Nexus'):
        cookie = (self.root / 'chains/Nexus/.cookie').read_text().strip()
        request = urllib.request.Request(
            f"http://127.0.0.1:{PORTS['rpc']}{route}?chainPath={chain}",
            headers={'Authorization': 'Basic ' + base64.b64encode(cookie.encode()).decode()})
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.load(response)

    def height(self, chain):
        try:
            return int(self.get('/api/chain/info', chain)['height'])
        except Exception:
            return -1

    def block(self, chain, height):
        return self.get(f'/api/block/{height}', chain)


def wait(what, condition, timeout, miner=None):
    deadline = time.monotonic() + timeout
    while not condition():
        assert miner is None or miner.poll() is None, f'the miner exited while waiting for {what}'
        assert time.monotonic() < deadline, f'timed out waiting for {what}'
        time.sleep(0.5)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--node-bin', type=pathlib.Path, required=True)
    parser.add_argument('--miner', type=pathlib.Path, default=ROOT / 'target/release/lattice-miner')
    parser.add_argument('--worker', type=pathlib.Path, default=ROOT / 'target/release/lattice-miner-gpu')
    parser.add_argument('--worker-backend', default='cpu')
    parser.add_argument('--cpu-workers', type=int, default=8)
    parser.add_argument('--hard-bits', type=int, default=30)
    parser.add_argument('--observe', type=int, default=60)
    parser.add_argument('--output', type=pathlib.Path, default=ROOT / 'target/node-integration.json')
    args = parser.parse_args()
    report = {'passed': False, 'binaries': {}}
    for path in [args.node_bin / 'lattice-node', args.node_bin / 'lattice', args.miner, args.worker]:
        report['binaries'][path.name] = hashlib.sha256(path.read_bytes()).hexdigest()
    report['nodeVersion'] = subprocess.run([str(args.node_bin / 'lattice-node'), '--version'],
                                           capture_output=True, text=True).stdout.strip()
    tmp = pathlib.Path(tempfile.mkdtemp(prefix='lattice-miner-integration-'))
    root, node, miner = tmp / 'node', Node(args.node_bin, tmp / 'node'), None
    log_path = tmp / 'miner.log'
    try:
        node.ctl('init')
        (root / 'lattice.json').write_text(json.dumps({
            'listen': PORTS['listen'], 'rpc': PORTS['rpc'], 'webSocket': 0, 'peers': [],
            'operatorListen': f"127.0.0.1:{PORTS['operator']}", 'operatorTokens': 'operator-tokens.json'}))
        node.ctl('up')
        wait('the node', lambda: node.height('Nexus') >= 0, 60)
        tokens = {}
        for name, routes in [('scoped', SCOPED), ('events', SCOPED + ['GET /mining/events'])]:
            out = tmp / f'{name}.token'
            node.ctl('tokens', 'create', name, *sum([['--route', r] for r in routes], []), '--out', str(out))
            tokens[name] = out.read_text().strip()
        addresses = {}
        for chain in ['Nexus', 'Nexus/C', 'Nexus/P', 'Nexus/P/G']:
            key = tmp / (chain.replace('/', '-') + '.key')
            subprocess.run([str(args.node_bin / 'lattice'), 'key', 'generate', '--out', str(key)],
                           check=True, capture_output=True)
            addresses[chain] = json.loads(key.read_text())['address']

        token_file, config = tmp / 'miner.token', tmp / 'miner.json'
        token_file.write_text(tokens['scoped'])
        settings = {'worker': 'cpu', 'workers': 4, 'batchSize': 100_000_000,
                    'recipients': {'Nexus': addresses['Nexus']}}
        config.write_text(json.dumps(settings))
        log = open(log_path, 'w')
        miner = subprocess.Popen(
            [str(args.miner), 'run', '--node', f"http://127.0.0.1:{PORTS['operator']}",
             '--token-file', str(token_file), '--config', str(config)],
            stdout=log, stderr=subprocess.STDOUT,
            env={**os.environ, 'LATTICE_MINER_BACKEND': args.worker_backend})
        miner_log = lambda: log_path.read_text()

        # 1. Scoped token: polling, Nexus, then children.
        wait('Nexus blocks', lambda: node.height('Nexus') >= 3, 120, miner)
        assert 'GET /mining/events answered HTTP 403' in miner_log(), miner_log()
        specs = {'Nexus/C': (1000, 1_000_000), 'Nexus/P': (3_600_000, 1), 'Nexus/P/G': (1000, 1_000_000)}
        for chain, (block_time, half_life) in specs.items():
            spec = tmp / (chain.replace('/', '-') + '.spec.json')
            spec.write_text(json.dumps({
                'maxNumberOfTransactionsPerBlock': 100, 'maxStateGrowth': 100_000, 'maxBlockSize': 1_000_000,
                'premine': 0, 'targetBlockTime': block_time, 'initialReward': 1000,
                'halvingInterval': 1_000_000, 'halfLife': half_life, 'wasmPolicies': []}))
            node.ctl('child', 'create', chain, '--spec', str(spec))
        settings['recipients'] = addresses
        config.write_text(json.dumps(settings))
        wait('blocks on every child', lambda: all(node.height(c) >= 2 for c in specs), 300, miner)
        for chain in ['Nexus'] + list(specs):
            latest = node.height(chain)
            assert node.block(chain, latest)['rewardRecipient'] == addresses[chain], chain
        report['scopedToken'] = {c: node.height(c) for c in ['Nexus'] + list(specs)}

        # 2. The token gains GET /mining/events, while the miner runs.
        token_file.write_text(tokens['events'])
        wait('the event stream', lambda: 'following GET /mining/events' in miner_log(), 60, miner)

        # 3. Harden P: minWork on Nexus tracks P's work, so each round's hit
        # (which must clear Nexus's minimum) also clears P.
        settings.update(workers=args.cpu_workers, batchSize=4_000_000_000)
        p_work = lambda: work(node.block('Nexus/P', node.height('Nexus/P'))['nextTarget'])
        started = time.monotonic()
        while (current := p_work()) < 1 << args.hard_bits:
            assert miner.poll() is None, 'the miner exited'
            assert time.monotonic() - started < 1800, 'P did not harden'
            if settings.get('minWork') != {'Nexus': str(current)}:
                settings['minWork'] = {'Nexus': str(current)}
                config.write_text(json.dumps(settings))
            time.sleep(0.5)
        report['warmup'] = {'seconds': round(time.monotonic() - started),
                            'heights': {c: node.height(c) for c in ['Nexus'] + list(specs)}}

        # 4. Hard parent: no minimum work; the external worker.
        settings.pop('minWork', None)
        settings.update(worker=str(args.worker), workers=1, batchSize=100_000_000)
        config.write_text(json.dumps(settings))
        # The round in flight finishes on the settings read before: its search
        # is capped at the binding minimum, so wait for the block that ends it.
        held = node.height('Nexus')
        deadline = time.monotonic() + 300
        while node.height('Nexus') == held:
            assert time.monotonic() < deadline, 'the warm-up round did not end'
            time.sleep(0.5)
        chains = ['Nexus'] + list(specs)
        before = {c: node.height(c) for c in chains}
        g_work = work(node.block('Nexus/P/G', before['Nexus/P/G'])['nextTarget'])
        hard_p_work = p_work()
        g_last, g_last_at, longest_gap = before['Nexus/P/G'], time.monotonic(), 0.0
        end = time.monotonic() + args.observe
        while time.monotonic() < end:
            assert miner.poll() is None, 'the miner exited'
            height = node.height('Nexus/P/G')
            now = time.monotonic()
            if height > g_last:
                longest_gap = max(longest_gap, now - g_last_at)
                g_last, g_last_at = height, now
            time.sleep(0.5)
        longest_gap = max(longest_gap, time.monotonic() - g_last_at)
        after = {c: node.height(c) for c in chains}
        advanced = {c: after[c] - before[c] for c in chains}
        report['hardParent'] = {
            'seconds': args.observe, 'advanced': advanced,
            'pWorkLog2': hard_p_work.bit_length() - 1, 'gWorkLog2': g_work.bit_length() - 1,
            'gLongestGapSeconds': round(longest_gap, 1)}
        # P can still land (it is hard, not unreachable); G must far outpace it.
        assert advanced['Nexus/P/G'] >= 10 * max(advanced['Nexus/P'], 1), advanced
        assert advanced['Nexus/P/G'] >= 10 and advanced['Nexus/C'] >= 10, advanced
        assert longest_gap < 20, longest_gap
        for chain in ['Nexus/C', 'Nexus/P/G']:
            for height in range(before[chain] + 1, after[chain] + 1):
                assert node.block(chain, height)['rewardRecipient'] == addresses[chain], (chain, height)

        miner.send_signal(signal.SIGTERM)
        assert miner.wait(timeout=30) == 0, miner_log()
        assert miner_log().rstrip().endswith('mining stopped'), miner_log()
        text = miner_log()
        report['minerLog'] = {
            'accepted': text.count('accepted tip='), 'childOnly': text.count('(childOnly)'),
            'notMining': text.count('NOT MINING'), 'deadlineExceeded': text.count('ROUND DEADLINE'),
            'workerFailures': text.count('failed (')}
        assert report['minerLog']['deadlineExceeded'] == 0 and report['minerLog']['workerFailures'] == 0, text
        report['passed'] = True
    finally:
        if miner and miner.poll() is None:
            miner.kill()
        try:
            node.ctl('down')
        except Exception:
            pass
        if log_path.exists():
            report['minerLogTail'] = log_path.read_text().splitlines()[-15:]
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + '\n')
        print(json.dumps(report, indent=2))
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == '__main__':
    main()
