import shutil
import time

name = "cloud status"


def run_push(context, stage):
    context.dvc("push", stage)


def run_status(context, stage):
    context.dvc("status", "-c", stage)


def run_gc(context):
    context.dvc("gc", "-w", "-f", "-c")


def run(context):
    data = context.generate_data(1024)
    dvc_file = f"{data}.dvc"
    context.dvc("push", dvc_file)

    yield "fresh status (nothing missing, 1024 files on the remote)", run_status, dvc_file

    context.generate_data(2048, name=data)

    yield "status (1k files missing, 1024 files on the remote)", run_status, dvc_file
    yield "push only new files (1024 new small files / 1024 existing small files)", run_push, dvc_file

    new_data = context.generate_data(4096, file_size=1)
    context.dvc("push", f"{new_data}.dvc")

    yield "fresh status (nothing missing, 2k + 4k files on the remote)", run_status, dvc_file
    context.generate_data(2049, name=data)
    yield "status (1 missing file, 2k + 4k files on the remote)", run_status, dvc_file
    yield "push only new files (1 new small file, 2k + 4k files on the remote)", run_push, dvc_file

    context.generate_data(2100, name=data)
    yield "status (51 missing file, 2k + 4k files on the remote)", run_status, dvc_file
    yield "push only new files (51 new small file, 2k + 4k files on the remote)", run_push, dvc_file
