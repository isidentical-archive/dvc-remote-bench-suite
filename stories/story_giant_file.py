import shutil
import time

name = "big files"


def run_push(context, stage):
    context.dvc("push", stage)


def run_pull(context, stage):
    context.dvc("pull", stage)


def run(context):
    data = context.generate_data(8, file_size="giant")

    yield "push (8 x 80 mb files)", run_push, f"{data}.dvc"

    context.generate_data(12, file_size="giant", name=data)

    yield "push (4 x 80 mb new files, 8 x 80 mb existing files)", run_push, f"{data}.dvc"

    shutil.rmtree(data)
    context.clear_cache()

    yield "pull (12 x 80 mb files)", run_pull, f"{data}.dvc"
