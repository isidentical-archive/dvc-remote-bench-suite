import os
import shutil
import time

name = "basic data cloud"


def run_push(context, stage):
    context.dvc("push", stage)


def run_pull(context, stage):
    context.dvc("pull", stage)


def run_gc(context):
    context.dvc("gc", "-w", "-f", "-c")


def run(context):
    data = context.generate_data(1024)
    dvc_file = f"{data}.dvc"

    yield "push (1024 small files)", run_push, dvc_file

    shutil.rmtree(data)
    shutil.rmtree(context.path / ".dvc" / "cache")

    yield "pull (1024 small files)", run_pull, dvc_file

    os.unlink(dvc_file)
    shutil.rmtree(data)
    context.clear_cache()
