import functools
import json
import os
import runpy
import shutil
import statistics
import subprocess
import tempfile
import time
import uuid
from argparse import ArgumentParser
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from pprint import pprint
from typing import Dict

ROOT_DIR = Path(__file__).parent
STORIES_DIR = ROOT_DIR / "stories"

_FILE_SIZE = {
    "nano": 64 * 1024,
    "mini": 256 * 1024,
    "small": 512 * 1024,
    "medium": 1024 * 1024,
    "big": 1024 * 1024 * 5,
    "giant": 1024 * 1024 * 80,
}
_GLOBAL_CONFIG = {}
_GLOBAL_CONFIG["base_tmp"] = os.getenv("BENCH_BASE_TMP", tempfile.gettempdir())


@contextmanager
def temp_location():
    base_dir = Path(f"{_GLOBAL_CONFIG['base_tmp']}/projects")
    base_dir.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(dir=base_dir) as tmp_dir:
        original_cwd = os.getcwd()
        try:
            os.chdir(tmp_dir)
            yield Path(tmp_dir)
        finally:
            os.chdir(original_cwd)


# https://github.com/iterative/dvc-bench/blob/0c7aad42e268dfb07661fbc8070cc4a65d79b3a1/benchmarks/base.py#L18-L35
def random_file(path, file_size):
    with open(path, "wb") as fobj:
        fobj.write(os.urandom(file_size))


def random_data_dir(num_files, file_size):
    dirname = "data_{}_{}".format(num_files, file_size)
    dir_path = os.path.join(f"{_GLOBAL_CONFIG['base_tmp']}/dvc_data/", dirname)

    if not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)
        fnames = [
            os.path.join(dir_path, "file_{}".format(str(i)))
            for i in range(num_files)
        ]
        with ThreadPoolExecutor(
            max_workers=max(os.cpu_count() / 2, 2)
        ) as pool:
            pool.map(random_file, fnames, len(fnames) * [file_size])
    return dirname, dir_path


@dataclass
class Context:
    env: Dict[str, str]
    path: Path

    def init(self):
        subprocess.check_call(["git", "init"])
        subprocess.check_call(["dvc", "init"])
        subprocess.check_call(
            ["dvc", "remote", "add", "-d", "default", self.get_remote_url()]
        )
        for key, value in self.env.items():
            subprocess.check_call(
                ["dvc", "remote", "modify", "default", key, value]
            )

    def get_remote_url(self):
        return os.path.join(self.env.pop("remote_url"), str(uuid.uuid4()))

    def dvc(self, *args, **kwargs):
        subprocess.check_call(["dvc", *args], **kwargs)

    def generate_data(self, num_files, file_size="small", name=None):
        dirname, data_dir = random_data_dir(
            num_files, _FILE_SIZE.get(file_size, file_size)
        )
        shutil.copytree(data_dir, name or dirname, dirs_exist_ok=True)
        self.dvc("add", name or dirname)
        return dirname

    def clear_cache(self):
        shutil.rmtree(self.path / ".dvc" / "cache")


def timed(func, *args, **kwargs):
    t0 = time.perf_counter()
    func(*args, **kwargs)
    return time.perf_counter() - t0


def run_stories(env, stories=None):
    results = defaultdict(dict)
    for story_path in STORIES_DIR.glob("story_*.py"):
        story_mod = runpy.run_path(story_path)
        story_name = story_mod.get("name", story_path.stem)
        if stories is not None and story_name not in stories:
            continue

        with temp_location() as temp_path:
            context = Context(env.copy(), temp_path)
            context.init()

            for scenerio, func, *args in story_mod["run"](context):
                results[story_name][scenerio] = timed(func, context, *args)

    return results


def merge_runs(results):
    new_results = defaultdict(dict)
    for stories in results:
        for story, scenerios in stories.items():
            for scenerio, time in scenerios.items():
                new_results[story].setdefault(scenerio, []).append(time)
    return new_results


def print_results(name, results):
    print(f"{name:=^80}")
    for story, scenerios in results.items():
        print(" " * 3, "Story:", story)
        for scenerio, times in scenerios.items():
            avg_time = round(statistics.mean(times), 4)
            message = f"{scenerio} took {avg_time} seconds"
            if len(times) >= 3:
                best, worst = min(times), max(times)
                message += (
                    f" (best: {round(best, 4)}, worst: {round(worst, 4)})"
                )

            print(" " * 7, message)


def run(env_file, repeat=3, stories=None):
    with open(env_file) as stream:
        environments = json.load(stream)

    if config := environments.pop("config", None):
        _GLOBAL_CONFIG.update(config)

    all_runs = {}
    for name, environment in environments.items():
        all_runs[name] = merge_runs(
            run_stories(environment, stories) for _ in range(repeat)
        )

    for name, results in all_runs.items():
        print_results(name, results)


def main():
    parser = ArgumentParser()
    parser.add_argument("env_file")
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--stories", type=str, nargs="*", default=None)

    options = parser.parse_args()
    return run(**vars(options))


if __name__ == "__main__":
    exit(main())
