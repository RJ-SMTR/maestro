import json
from os import getenv

import jinja2
import yaml

CMD_TEMPLATE = jinja2.Template(
    "kubectl get secret {{ secret }} -n {{ namespace }} -o jsonpath=\"{.data}\"")


def build_command():
    secret = getenv("SECRET")
    namespace = getenv("NAMESPACE")
    return CMD_TEMPLATE.render(secret=secret, namespace=namespace)


def run_shell_command(cmd):
    import subprocess
    return subprocess.check_output(cmd, shell=True).decode("utf-8")


def get_secret_data():
    return json.loads(run_shell_command(build_command()))


def build_yaml():
    d = {}
    d["apiVersion"] = "v1"
    d["kind"] = "Secret"
    d["metadata"] = {}
    d["metadata"]["name"] = getenv("SECRET")
    d["data"] = get_secret_data()
    with open("secrets_backup.yaml", "w") as f:
        yaml.dump(d, f)


if __name__ == "__main__":
    build_yaml()
