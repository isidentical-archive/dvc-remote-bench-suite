import shutil
import time

name = "basic data cloud"


def run_push(context, stage):
    context.dvc("push", stage)


def run_pull(context, stage):
    context.dvc("pull", stage)


def run(context):
    data = context.generate_data(1024)
    yield "push (1024 small files)", run_push, f"{data}.dvc"
    shutil.rmtree(data)
    shutil.rmtree(context.path / ".dvc" / "cache")
    yield "pull (1024 small files)", run_pull, f"{data}.dvc"
