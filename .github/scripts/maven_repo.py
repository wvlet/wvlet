#!/usr/bin/env python3
"""Helpers for the wvlet Maven repository (https://repo.wvlet.org/maven2) on Cloudflare R2.

Copied from wvlet/uni (.github/scripts/maven_repo.py); keep the two copies in sync, since uni,
airframe and wvlet share the bucket.

Subcommands:

  check STAGING REPO
      Fail if a version staged under STAGING (a Maven layout such as target/sona-staging) already has
      a POM in REPO. A published version is never overwritten, so repo.wvlet.org keeps serving the
      same files that may already have been copied to Maven Central.

  metadata STAGING REPO OUT
      For every artifact directory under STAGING, list the version directories that REPO holds for
      it and write maven-metadata.xml (with .md5 and .sha1 checksums) to the same relative path under
      OUT. Run it after the artifacts are uploaded. The metadata is rebuilt from what the repository
      holds, not merged with the previous file, so two releases running at once (uni and airframe
      share the bucket) need no lock: the later one lists both versions.

  REPO is an `s3://bucket/prefix` URL (read with the aws CLI; set AWS_ENDPOINT_URL for R2) or a
  local directory.

  bundle REPO_DIR VERSION OUT_ZIP
      Zip every file of VERSION under REPO_DIR (a Maven layout) into a Maven Central Portal bundle,
      leaving out maven-metadata files, which Central generates itself.

Version directories are found by their POM: a directory that holds `<artifact>-<version>.pom` is a
version directory, and its parent is the artifact directory.
"""

import hashlib
import re
import subprocess
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path

METADATA = "maven-metadata.xml"


def version_key(v: str):
    # Versions are YYYY.milestone.patch. Compare the numbers numerically; a suffixed version
    # (2026.2.1-RC1) sorts before the plain release with the same numbers, as in Maven.
    base, _, suffix = v.partition("-")
    nums = [int(p) if p.isdigit() else -1 for p in base.split(".")]
    return (nums, 0 if suffix else 1, suffix)


def find_versions(staging: Path):
    """Yield (artifact_dir_relpath, group_id, artifact_id, version) for every staged POM."""
    for pom in sorted(staging.rglob("*.pom")):
        version_dir = pom.parent
        artifact_dir = version_dir.parent
        version = version_dir.name
        artifact_id = artifact_dir.name
        if pom.name != f"{artifact_id}-{version}.pom":
            continue
        group_path = artifact_dir.parent.relative_to(staging)
        yield artifact_dir.relative_to(staging), ".".join(group_path.parts), artifact_id, version


def list_versions(repo: str, rel: Path):
    """Version directories REPO holds for one artifact directory."""
    if repo.startswith("s3://"):
        out = subprocess.run(
            ["aws", "s3", "ls", f"{repo.rstrip('/')}/{rel.as_posix()}/"],
            capture_output=True,
            text=True,
        )
        # `aws s3 ls` exits with 1 when the prefix does not exist yet (a new artifact)
        if out.returncode not in (0, 1):
            sys.exit(out.stderr)
        return [m.group(1) for m in re.finditer(r"^\s*PRE (.+)/$", out.stdout, re.MULTILINE)]
    path = Path(repo) / rel
    return [d.name for d in path.iterdir() if d.is_dir()] if path.is_dir() else []


def has_pom(repo: str, rel: Path, artifact_id: str, version: str) -> bool:
    key = f"{rel.as_posix()}/{version}/{artifact_id}-{version}.pom"
    if repo.startswith("s3://"):
        out = subprocess.run(["aws", "s3", "ls", f"{repo.rstrip('/')}/{key}"], capture_output=True)
        return out.returncode == 0
    return (Path(repo) / key).exists()


def render(group_id: str, artifact_id: str, versions) -> bytes:
    ordered = sorted(set(versions), key=version_key)
    latest = ordered[-1]
    now = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        "<metadata>",
        f"  <groupId>{group_id}</groupId>",
        f"  <artifactId>{artifact_id}</artifactId>",
        "  <versioning>",
        f"    <latest>{latest}</latest>",
        f"    <release>{latest}</release>",
        "    <versions>",
        *[f"      <version>{v}</version>" for v in ordered],
        "    </versions>",
        f"    <lastUpdated>{now}</lastUpdated>",
        "  </versioning>",
        "</metadata>",
        "",
    ]
    return "\n".join(lines).encode("utf-8")


def cmd_check(staging: str, repo: str):
    published = [
        f"{artifact_id}:{version}"
        for rel, _, artifact_id, version in find_versions(Path(staging))
        if has_pom(repo, rel, artifact_id, version)
    ]
    if published:
        sys.exit(f"Already published, refusing to overwrite: {', '.join(published)}")


def cmd_metadata(staging: str, repo: str, out: str):
    out_path = Path(out)
    artifacts = {}
    for rel, group_id, artifact_id, version in find_versions(Path(staging)):
        artifacts.setdefault(rel, (group_id, artifact_id, set()))[2].add(version)
    if not artifacts:
        sys.exit(f"No POM files found under {staging}")
    for rel, (group_id, artifact_id, versions) in sorted(artifacts.items()):
        data = render(group_id, artifact_id, list_versions(repo, rel) + sorted(versions))
        target = out_path / rel / METADATA
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
        for algo in ("md5", "sha1"):
            Path(f"{target}.{algo}").write_text(hashlib.new(algo, data).hexdigest())
        print(f"{rel.as_posix()}: {', '.join(sorted(versions, key=version_key))}")


def cmd_bundle(repo_dir: str, version: str, out_zip: str):
    repo = Path(repo_dir)
    files = [
        f
        for f in sorted(repo.rglob("*"))
        if f.is_file() and f.parent.name == version and not f.name.startswith("maven-metadata")
    ]
    if not any(f.name.endswith(".pom") for f in files):
        sys.exit(f"No POM files for version {version} under {repo_dir}")
    with zipfile.ZipFile(out_zip, "w", zipfile.ZIP_DEFLATED) as z:
        for f in files:
            z.write(f, f.relative_to(repo).as_posix())
    print(f"Bundled {len(files)} files for {version} into {out_zip}")


if __name__ == "__main__":
    commands = {"check": (cmd_check, 2), "metadata": (cmd_metadata, 3), "bundle": (cmd_bundle, 3)}
    if len(sys.argv) < 2 or sys.argv[1] not in commands or len(sys.argv) - 2 != commands[sys.argv[1]][1]:
        sys.exit(__doc__)
    fn, _ = commands[sys.argv[1]]
    fn(*sys.argv[2:])
