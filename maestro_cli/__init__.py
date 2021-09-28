__all__ = ["app"]

import os
import subprocess
from typing import Callable
from threading import Thread
from functools import partial

import typer
from loguru import logger


app = typer.Typer()


def do_nothing(text: str):
    pass


def remove_last_newline(text: str) -> str:
    return text[:-1] if text.endswith("\n") else text


def log(text: str, prefix: str = "", color: str = "white"):
    logger.info(f"{prefix} {remove_last_newline(text)}", color=color)


def run_shell_command(command: str, stdout_callback: Callable = do_nothing) -> int:
    popen = subprocess.Popen(
        command, shell=True, stdout=subprocess.PIPE, universal_newlines=True)
    for stdout_line in iter(popen.stdout.readline, ""):
        stdout_callback(stdout_line)
    popen.stdout.close()
    return_code = popen.wait()
    if return_code:
        raise subprocess.CalledProcessError(return_code, command)
    return return_code


def copy_workspace_file():
    run_shell_command(
        f"cp workspace_local.yaml workspace.yaml", partial(log, prefix="[WORKSPACE FILE]"))


def load_env_file(env_file: str):
    with open(env_file) as file:
        for line in file:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()
            os.environ[key] = value


def install_dependencies():
    run_shell_command(
        f"/usr/bin/env python -m pip install -r requirements.txt", partial(log, prefix="[DEPENDENCIES]"))


def setup_docker():
    run_shell_command(
        f"docker-compose -f docker-compose-local.yaml up -d", partial(log, prefix="[DOCKER]"))


def docker_down():
    run_shell_command("docker-compose -f docker-compose-local.yaml down",
                      partial(log, prefix="[DOCKER]"))


def run_daemon():
    while True:
        try:
            run_shell_command("dagster-daemon run",
                              partial(log, prefix="[DAEMON]"))
        except:
            pass


def run_dagit():
    while True:
        try:
            run_shell_command("dagit", partial(log, prefix="[DAGIT]"))
        except:
            pass


def run_grpc():
    while True:
        try:
            run_shell_command(
                "dagster api grpc -h 0.0.0.0 -p 4000 -f repositories/repository.py", partial(log, prefix="[GRPC]"))
        except:
            pass


def run_threaded(func):
    def wrapper(*args, **kwargs):
        thread = Thread(target=func, args=args, kwargs=kwargs)
        thread.start()
    return wrapper


@app.command()
def setup():
    """
    Setup environment for maestro
    """
    install_dependencies()
    setup_docker()
    copy_workspace_file()


@app.command()
def up():
    """
    Run maestro setup locally
    """
    load_env_file(".env_local")
    run_threaded(run_daemon)()
    run_threaded(run_dagit)()
    run_threaded(run_grpc)()


@app.command()
def down():
    """
    Shutdown local setup
    """
    docker_down()
