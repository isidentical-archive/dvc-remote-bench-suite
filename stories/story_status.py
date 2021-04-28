import shutil
import time

name = "basic data cloud"


def run_push(context, stage):
    context.dvc("push", stage)


def run_status(context, stage):
    context.dvc("status", "-c", stage)


def run(context):
    data = context.generate_data(1024)
    dvc_file = f"{data}.dvc"
    context.dvc("push", dvc_file)
    yield "fresh status (nothing missing on the remote)", run_status, dvc_file
    context.generate_data(2048, name=data)
    yield "status (1024 files missing on the remote)", run_status, dvc_file
    yield "push only new files (1024 new small files / 1024 existing small files)", run_push, dvc_file
