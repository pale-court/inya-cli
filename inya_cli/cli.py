from io import BytesIO
from typing import Optional
from unicodedata import decimal
import atomicwrites
import hashlib
import json
import os
import platform
import ndjson
from pathlib import Path, PurePosixPath
from pydantic import BaseModel, HttpUrl
import requests
from rich.progress import track
import sys
import zstandard as zstd


class StageArgs(BaseModel):
    remote_url: HttpUrl
    root: Path
    build: int
    game_root: Optional[Path]


class SteamHashes:
    def __init__(self, game_dir):
        self.game_dir = game_dir
        bundles2 = game_dir / "Bundles2"
        self.path_by_hash = {}
        for root, dirs, files in track(
            os.walk(bundles2), description="Hashing external game data"
        ):
            root = Path(root)
            for file in files:
                full_path = root / file
                rel_path = full_path.relative_to(game_dir)
                with (root / file).open("rb") as fh:
                    digest = hashlib.sha256(fh.read()).hexdigest()
                    self.path_by_hash[digest] = rel_path


def stage():
    args = StageArgs(
        remote_url=sys.argv[1],
        root=sys.argv[2],
        build=sys.argv[3],
        game_root=sys.argv[4] if len(sys.argv) >= 5 else None,
    )

    game_hashes = None
    if args.game_root:
        game_hashes = SteamHashes(Path(args.game_root))

    args.root.mkdir(parents=True, exist_ok=True)

    data_dir = args.root / "storage/data"
    data_dir.mkdir(parents=True, exist_ok=True)
    for i in range(0x100):
        (data_dir / f"{i:02x}").mkdir(exist_ok=True)

    index_dir = args.root / "storage/index"
    index_dir.mkdir(parents=True, exist_ok=True)

    sess = requests.session()
    builds_url = f"{args.remote_url}/poe-meta/builds/public"
    builds = sess.get(builds_url).json()
    build = builds[str(args.build)]
    manifest_id = build["manifests"]["238961"]

    print("Fetching index")
    index_path = index_dir / f"{manifest_id}-loose.ndjson"
    index_data = None
    if not index_path.exists():
        index_url = f"{args.remote_url}/poe-index/238961/{manifest_id}-loose.ndjson.zst"
        index_resp = sess.get(index_url)
        index_data = bytearray()
        dctx = zstd.ZstdDecompressor()
        with atomicwrites.atomic_write(index_path, mode='wb') as fh:
            for chunk in dctx.read_to_iter(index_resp.content):
                index_data.extend(chunk)
                fh.write(chunk)
        del index_resp

    build_dir = args.root / f"build/{args.build}"
    build_dir.mkdir(parents=True, exist_ok=True)

    should_hardlink = platform.system() == "Windows"

    entries = []
    from_data = {}
    from_game = {}
    from_web = {}
    fh = BytesIO(index_data) if index_data else index_path.open("rb")
    for entry in track(ndjson.reader(fh), description="Enumerating index"):
        path = entry["path"]
        if not path.startswith("Bundles2"):
            continue
        path = PurePosixPath(entry["path"])
        sha256 = entry["sha256"]
        subdir = sha256[:2]
        targetname = data_dir / f"{subdir}/{sha256}.bin"

        e = {
            "path": path,
            "sha256": sha256,
            "size": entry["size"],
            "target": targetname,
        }
        entries.append(e)

        if targetname.exists():
            from_data[path] = e
        elif game_hashes and e["sha256"] in game_hashes.path_by_hash:
            from_game[path] = e
        else:
            from_web[path] = e

    for entry in track(from_game.values(), description="Copying data from game dir"):
        targetname = entry["target"]
        if not targetname.exists():
            with atomicwrites.atomic_write(targetname, mode="wb") as fh:
                src_path = game_hashes.path_by_hash.get(entry["sha256"])
                fh.write((game_hashes.game_dir / src_path).read_bytes())

    fh = BytesIO(index_data) if index_data else index_path.open("rb")
    for entry in track(from_web.values(), description="Fetching data"):
        sha256 = entry["sha256"]
        subdir = sha256[:2]
        targetname = entry["target"]

        if not targetname.exists():
            with atomicwrites.atomic_write(targetname, mode="wb") as fh:
                remote_url = f"{args.remote_url}/poe-data/{subdir}/{sha256}.bin"
                with sess.get(remote_url) as resp:
                    fh.write(resp.content)

    fh = BytesIO(index_data) if index_data else index_path.open("rb")
    for entry in track(entries, description="Linking data"):
        path = entry["path"]
        sha256 = entry["sha256"]
        subdir = sha256[:2]
        linkname = build_dir / path
        targetname = data_dir / f"{subdir}/{sha256}.bin"

        if should_hardlink:
            if not linkname.exists():
                linkname.parent.mkdir(parents=True, exist_ok=True)
                linkname.hardlink_to(targetname)
        else:
            try:
                linkname.lstat()
            except:
                linkname.parent.mkdir(parents=True, exist_ok=True)
                linkname.symlink_to(targetname)
