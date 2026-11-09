#!/usr/bin/env python3
"""Inspect private GoReleaser snapshot outputs; never publish or reset shared state."""
import argparse
import hashlib
import json
import os
import pathlib
import re
import subprocess
import tarfile
import time
import urllib.error
import urllib.request
import uuid
import zipfile


def command(args, timeout=120, check=True):
    return subprocess.run(args, text=True, stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT, timeout=timeout, check=check).stdout


def digest(data):
    return hashlib.sha256(data).hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dist', type=pathlib.Path, default=pathlib.Path('dist'))
    parser.add_argument('--output', type=pathlib.Path, required=True)
    parser.add_argument('--govulncheck', required=True)
    args = parser.parse_args()
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    dist = args.dist.resolve()
    sha = command(['git', 'rev-parse', 'HEAD']).strip()
    report = {'sha': sha, 'status': 'incomplete', 'archives': [], 'images': [],
              'scope': 'Private snapshot archives and local images; no tag, release or registry publication. Linux/arm64 execution uses QEMU on GitHub amd64.',
              'go': command(['go', 'version']).strip(),
              'scanner': command([args.govulncheck, '-version']).strip(),
              'goreleaser': command(['goreleaser', '--version']).strip()}
    owned = []
    network = None
    try:
        metadata = json.loads((dist / 'metadata.json').read_text())
        version = metadata['version']
        report['metadata'] = metadata
        checksums = {}
        for line in (dist / 'checksums.txt').read_text().splitlines():
            checksum, name = line.split(None, 1)
            name = name.lstrip('*')
            if pathlib.PurePosixPath(name).name != name or not re.fullmatch('[a-f0-9]{64}', checksum):
                raise RuntimeError('Invalid checksum inventory')
            checksums[name] = checksum
        for name, checksum in checksums.items():
            if digest((dist / name).read_bytes()) != checksum:
                raise RuntimeError('Checksum inventory mismatch: ' + name)
        binaries = {}
        for archive in sorted(dist.iterdir()):
            match = re.fullmatch(r'emitlane_.+_(darwin|linux|windows)_(amd64|arm64)\.(tar\.gz|zip)', archive.name)
            if not match:
                continue
            target_os, arch, suffix = match.groups()
            data = archive.read_bytes()
            if checksums.get(archive.name) != digest(data):
                raise RuntimeError('Archive checksum mismatch: ' + archive.name)
            executable = 'emitlane.exe' if target_os == 'windows' else 'emitlane'
            contents = []
            if suffix == 'zip':
                with zipfile.ZipFile(archive) as handle:
                    if handle.testzip() is not None:
                        raise RuntimeError('ZIP CRC failure')
                    for item in handle.infolist():
                        contents.append(item.filename)
                    candidates = [n for n in contents if n == executable]
                    if len(candidates) != 1:
                        raise RuntimeError('Unexpected executable inventory')
                    binary = handle.read(executable)
            else:
                with tarfile.open(archive, 'r:gz') as handle:
                    members = handle.getmembers()
                    contents = [n.name for n in members]
                    candidates = [n for n in members if n.name == executable and n.isfile()]
                    if len(candidates) != 1:
                        raise RuntimeError('Unexpected executable inventory')
                    # Read every member, including the tail of the gzip stream.
                    for item in members:
                        if item.isfile():
                            payload = handle.extractfile(item).read()
                            if item.name == executable:
                                binary = payload
                import gzip
                with gzip.open(archive, 'rb') as handle:
                    while handle.read(1024 * 1024):
                        pass
            if any(pathlib.PurePosixPath(n).is_absolute() or '..' in pathlib.PurePosixPath(n).parts for n in contents):
                raise RuntimeError('Unsafe archive member')
            if not any(pathlib.PurePosixPath(n).name.startswith('LICENSE') for n in contents):
                raise RuntimeError('Missing license in archive')
            key = target_os + '/' + arch
            if key in binaries:
                raise RuntimeError('Duplicate archive target')
            path = out / (target_os + '-' + arch + '-' + executable)
            path.write_bytes(binary)
            path.chmod(0o700)
            build = command(['go', 'version', '-m', str(path)])
            (out / (target_os + '-' + arch + '-build.txt')).write_text(build)
            if 'GOOS=' + target_os not in build or 'GOARCH=' + arch not in build or sha not in build or version not in build:
                raise RuntimeError('Packaged build identity/target mismatch')
            scan = subprocess.run([args.govulncheck, '-mode=binary', str(path)],
                                  stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=300)
            (out / (target_os + '-' + arch + '-vulnerability.txt')).write_text(scan.stdout)
            report['archives'].append({'file': archive.name, 'target': key, 'archive_sha256': digest(data),
                                       'binary_sha256': digest(binary), 'members': contents, 'scan_exit': scan.returncode})
            if scan.returncode:
                raise RuntimeError('Packaged binary vulnerability scan failed: ' + key)
            binaries[key] = path
        if set(binaries) != {s + '/' + a for s in ['darwin', 'linux', 'windows'] for a in ['amd64', 'arm64']}:
            raise RuntimeError('Expected six packaged targets')
        image_ids = command(['docker', 'image', 'ls', '-q', '--no-trunc', 'ghcr.io/emitlane/emitlane']).split()
        by_arch = {}
        for image_id in sorted(set(image_ids)):
            info = json.loads(command(['docker', 'image', 'inspect', image_id]))[0]
            labels = info['Config'].get('Labels') or {}
            if labels.get('org.opencontainers.image.revision') != sha:
                continue
            arch = info['Architecture']
            if info['Os'] != 'linux' or arch not in ['amd64', 'arm64'] or arch in by_arch:
                raise RuntimeError('Unexpected snapshot image platforms')
            if labels.get('org.opencontainers.image.version') != version:
                raise RuntimeError('Image version differs from snapshot metadata')
            if info['Config']['User'] not in ['nonroot:nonroot', '65532:65532'] or info['Config']['Entrypoint'] != ['/emitlane']:
                raise RuntimeError('Image execution identity differs from release Dockerfile')
            container = command(['docker', 'create', '--platform', 'linux/' + arch, image_id, 'version']).strip()
            owned.append(container)
            copied = out / ('image-' + arch)
            command(['docker', 'cp', container + ':/emitlane', str(copied)])
            if copied.read_bytes() != binaries['linux/' + arch].read_bytes():
                raise RuntimeError('Image and packaged executable bytes differ')
            text = command(['docker', 'run', '--rm', '--network=none', '--platform', 'linux/' + arch, image_id, 'version'])
            if sha not in text or version not in text:
                raise RuntimeError('Image version output differs from approved source')
            (out / ('image-' + arch + '-version.txt')).write_text(text)
            (out / ('image-' + arch + '-help.txt')).write_text(command(['docker', 'run', '--rm', '--network=none', '--platform', 'linux/' + arch, image_id, 'help']))
            (out / ('image-' + arch + '-inspect.json')).write_text(json.dumps(info, indent=2))
            by_arch[arch] = image_id
            report['images'].append({'id': image_id, 'arch': arch, 'binary_sha256': digest(copied.read_bytes()),
                                     'revision': sha, 'version': version, 'user': info['Config']['User'], 'rootfs': info['RootFS']})
        if set(by_arch) != {'amd64', 'arm64'}:
            raise RuntimeError('Both local snapshot image platforms are required')
        # Only fresh fixture-owned containers and network are ever cleaned up.
        prefix = 'emitlane-artifact-' + uuid.uuid4().hex[:10]
        network = prefix
        command(['docker', 'network', 'create', network])
        pg = prefix + '-pg'
        owned.append(pg)
        command(['docker', 'run', '-d', '--name', pg, '--network', network, '--network-alias', 'postgres',
                 '--shm-size=512m', '-e', 'POSTGRES_USER=emitlane', '-e', 'POSTGRES_PASSWORD=emitlane',
                 '-e', 'POSTGRES_DB=emitlane', 'postgres:16-alpine'])
        kafka = prefix + '-kafka'
        owned.append(kafka)
        image = re.search(r'kafkaImage\s*=\s*"([^"]+)"', pathlib.Path('cmd/emitlane-soak/model.go').read_text()).group(1)
        env = {'KAFKA_NODE_ID': '1', 'KAFKA_PROCESS_ROLES': 'broker,controller',
               'KAFKA_LISTENERS': 'PLAINTEXT://:9092,CONTROLLER://:9093',
               'KAFKA_ADVERTISED_LISTENERS': 'PLAINTEXT://kafka:9092',
               'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP': 'PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT',
               'KAFKA_INTER_BROKER_LISTENER_NAME': 'PLAINTEXT', 'KAFKA_CONTROLLER_LISTENER_NAMES': 'CONTROLLER',
               'KAFKA_CONTROLLER_QUORUM_VOTERS': '1@kafka:9093', 'KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR': '1',
               'KAFKA_TRANSACTION_STATE_LOG_MIN_ISR': '1', 'KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR': '1',
               'CLUSTER_ID': 'MkU3OEVBNTcwNTJENDM2Qk'}
        docker = ['docker', 'run', '-d', '--name', kafka, '--network', network, '--network-alias', 'kafka']
        for key, val in env.items():
            docker.extend(['-e', key + '=' + val])
        command(docker + [image])
        for name, probe in [(pg, ['pg_isready', '-U', 'emitlane', '-d', 'emitlane']),
                            (kafka, ['/opt/kafka/bin/kafka-topics.sh', '--bootstrap-server', 'kafka:9092', '--list'])]:
            deadline = time.monotonic() + 120
            while True:
                result = subprocess.run(['docker', 'exec', name] + probe, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=30)
                if result.returncode == 0:
                    break
                if time.monotonic() > deadline:
                    raise RuntimeError('Fixture readiness deadline: ' + name)
                time.sleep(2)
        settings = json.loads(command(['docker', 'exec', pg, 'psql', '-U', 'emitlane', '-d', 'emitlane', '-At', '-c',
            "SELECT json_object_agg(name,setting) FROM pg_settings WHERE name IN ('server_version','fsync','synchronous_commit','full_page_writes')"]))
        if not settings['server_version'].startswith('16.') or any(settings[k] != 'on' for k in ['fsync', 'synchronous_commit', 'full_page_writes']):
            raise RuntimeError('Unexpected PostgreSQL version or durability')
        report['postgres_settings'] = settings
        connection = 'EMITLANE_DATABASE_URL=postgres://emitlane:emitlane@postgres:5432/emitlane?sslmode=disable'
        migration = command(['docker', 'run', '--rm', '--network', network, '-e', connection, by_arch['amd64'], 'migrate', 'up'])
        (out / 'migration.txt').write_text(migration)
        app = prefix + '-app'
        owned.append(app)
        command(['docker', 'run', '-d', '--name', app, '--network', network, '-p', '127.0.0.1::8080',
                 '-e', connection, '-e', 'EMITLANE_KAFKA_BROKERS=kafka:9092', '-e', 'EMITLANE_HTTP_ADDR=:8080', by_arch['amd64'], 'run'])
        info = json.loads(command(['docker', 'inspect', app]))[0]
        port = info['NetworkSettings']['Ports']['8080/tcp'][0]['HostPort']
        for endpoint in ['healthz', 'readyz']:
            deadline = time.monotonic() + 60
            while True:
                try:
                    with urllib.request.urlopen('http://127.0.0.1:' + port + '/' + endpoint, timeout=5) as response:
                        if response.status == 200:
                            break
                except (OSError, urllib.error.URLError):
                    pass
                if time.monotonic() > deadline:
                    raise RuntimeError('Packaged runtime readiness deadline: ' + endpoint)
                time.sleep(1)
        command(['docker', 'stop', '--time', '35', app], timeout=45)
        state = json.loads(command(['docker', 'inspect', app]))[0]['State']
        if state['ExitCode'] != 0 or state['OOMKilled']:
            raise RuntimeError('Packaged runtime did not shut down gracefully')
        report['runtime_startup'] = {'platform': 'linux/amd64', 'healthz': 200, 'readyz': 200,
                                     'exit_code': state['ExitCode'], 'kafka_image': image,
                                     'scope': 'Fresh durable PG16/schema5 and pinned JVM Kafka, plaintext private fixture; no application pilot or production TLS qualification.'}
        report['status'] = 'passed'
    except BaseException as exc:
        report['status'] = 'failed'
        report['error'] = str(exc)
        raise
    finally:
        cleanup_errors = []
        for name in owned:
            try:
                text = command(['docker', 'logs', name], check=False)
                (out / ('container-' + name + '.log')).write_text(text)
                command(['docker', 'rm', '-f', name])
            except BaseException as exc:
                cleanup_errors.append(str(exc))
        if network:
            try:
                command(['docker', 'network', 'rm', network])
            except BaseException as exc:
                cleanup_errors.append(str(exc))
        report['cleanup_errors'] = cleanup_errors
        if cleanup_errors:
            report['status'] = 'failed'
        (out / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
        if cleanup_errors:
            raise RuntimeError('Owned artifact fixture cleanup failed')


if __name__ == '__main__':
    main()
