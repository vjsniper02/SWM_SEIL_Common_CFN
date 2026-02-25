import logging
import os
import toml
from copy import deepcopy
from invoke import task
from invoke.exceptions import Exit
from os import path, listdir
from typing import Iterator, Tuple

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("tasks")


@task
def clean(c, fn):
    c.run(f"rm -rf {fn}/dist/")


@task
def cleanall(c):
    c.run(f"find . -type d -name 'dist' -exec rm -r {{}} +")


@task
def build(c, fn, dockertag, reportsdir):
    cfg_file = f"{fn}/pyproject.toml"
    if not path.isdir(fn):
        raise Exit(f"path for {fn} not found")
    if not path.isfile(cfg_file):
        raise Exit(f"{fn}/pyproject.toml not found")

    os.mkdir(os.path.join(fn, "dist"))
    # Prepare a new pyproject.yaml and poetry.lock
    config = PyProject(fn)
    with open(path.join(fn, "dist/pyproject.toml"), "w") as f:
        f.write(config.updated_pyproject())
    with open(path.join(fn, "dist/poetry.lock"), "w") as f:
        f.write(config.updated_poetry())

    # Copy dependencies
    for dep in config.deps_to_copy():
        logger.info(f"Copying {dep[0]} to {dep[1]}")
        c.run(f"mkdir -m 0755 -p {dep[1]}")
        c.run(f"cp -pR {fn}/{dep[0]}/* {dep[1]}")

    # Copy source
    logger.info("Copying source")
    for filename in listdir(fn):
        if filename not in ["pyproject.toml", "poetry.lock", "dist"]:
            c.run(f"cp -pR {fn}/{filename} {fn}/dist/")

    logger.info(f"Test reports to {reportsdir}")
    c.run(
        f"DOCKER_BUILDKIT=1 docker build --no-cache -t {dockertag} --target export-stage -f Dockerfile {fn} -o {reportsdir}"
    )
    logger.info("Docker build")
    c.run(f"docker build -t {dockertag} -f Dockerfile {fn}")


class PyProject:
    pyproject_toml: dict
    poetry_lock: dict
    dst_dir: str

    def __init__(self, project_dir: str):
        with open(path.join(project_dir, "pyproject.toml")) as f:
            self.pyproject_toml = toml.load(f)
        with open(path.join(project_dir, "poetry.lock")) as f:
            self.poetry_lock = toml.load(f)
        self.dst_dir = project_dir

    def deps_to_copy(self) -> Iterator[Tuple[str, str]]:
        for dependency in self.pyproject_toml["tool"]["poetry"]["dependencies"].items():
            if isinstance(dependency[1], dict) and "path" in dependency[1].keys():
                src = dependency[1]["path"]
                dst = path.join(
                    self.dst_dir,
                    "dist/lib",
                    src.split("/lib/")[1],
                )
                yield src, dst

    def updated_pyproject(self) -> str:
        new_config = deepcopy(self.pyproject_toml)
        for dependency in new_config["tool"]["poetry"]["dependencies"].items():
            if isinstance(dependency[1], dict) and "path" in dependency[1].keys():
                dependency[1]["path"] = "lib/" + dependency[1]["path"].split("/lib/")[1]
        return toml.dumps(new_config)

    def updated_poetry(self) -> str:
        new_poetry_lock = deepcopy(self.poetry_lock)
        for package in new_poetry_lock["package"]:
            if "source" in package.keys() and package["source"]["type"] == "directory":
                package["source"]["url"] = (
                    "lib/" + package["source"]["url"].split("/lib/")[1]
                )
        return toml.dumps(new_poetry_lock)
