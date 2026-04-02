import glob
from io import TextIOWrapper
import logging
from pathlib import Path
import shutil
import subprocess
import sys
from typing import Any, Iterable, cast

from karps.config import Env, get_env
from karps.util import yaml
from karps.util.git import GitRepo


logger = logging.getLogger(__name__)

__all__ = ["main"]


def main():
    """Small CLI for Karp-s backend. Parses CLI args (sys.argv).

    Supported subcommands:
    - init: create the needed structure (also run for every other command)
    - add <resource>: add a resource from the incoming directory
    - reload: reloads the workers of the API
    - reconfigure: recreates the configuration based on each resource in the incoming directory
    """
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    config: Env = get_env()
    # create directory structure if not done
    main_dir, repo = create(config)
    if sys.argv[1] == "init":
        return
    if sys.argv[1] == "add":
        resource_id = sys.argv[2]
        resource_dir = main_dir / "incoming" / resource_id
        process_resource(main_dir, resource_dir, repo)
    elif sys.argv[1] == "reload":
        restart_workers(config)
    elif sys.argv[1] == "reconfigure":
        reconfigure(main_dir, repo)
        restart_workers(config)
    elif sys.argv[1] == "remove":
        resource_id = sys.argv[2]
        resource_dir = main_dir / "incoming" / resource_id
        shutil.rmtree(resource_dir, ignore_errors=True)
        reconfigure(main_dir, repo)
        restart_workers(config)
    else:
        raise RuntimeError(f"karp-s-cli: commands not supported {sys.argv}")


def restart_workers(config: Env):
    # will restart all gunicorn workers
    p = subprocess.run(["make", "reload"], cwd=config.base_path, capture_output=True, check=False, encoding="utf-8")
    if p.returncode:
        logger.warning("failed to reload karp-s-backend")
        raise RuntimeError(f"stdout: {p.stdout}, stderr: {p.stderr}")
    else:
        logger.info("karp-s-backend reloaded")


def reconfigure(main_dir: Path, repo):
    for path in glob.glob(str(main_dir / "resources/*")):
        Path(path).unlink()
    for path in glob.glob(str(main_dir / "incoming/*")):
        resource_dir = Path(path)
        if resource_dir.is_dir():
            process_resource(main_dir, resource_dir, repo)


def process_resource(
    main_dir: Path, resource_dir: Path, repo: GitRepo, done_file: Path | None = None, background=False
):
    # this backend instance's field configuration
    backend_fields_config = main_dir / "fields.yaml"
    # general resource information
    karps_resource_config = resource_dir / "resource.yaml"
    # resource fields information
    resource_fields_config = resource_dir / "fields.yaml"
    # other information about resource, for example tags
    global_config = resource_dir / "global.yaml"
    try:
        # this updates config.yaml with new information from the resource
        resource_id = _update_config(main_dir / "config.yaml", karps_resource_config, global_config)
        # this merges all the current resource field configs into one big file, taking into account
        # that fields.yaml may already contain translated labels etc
        _update_fields(resource_id, backend_fields_config, resource_fields_config)
        # finally copy the resource config to the resource dir
        shutil.copy(karps_resource_config, main_dir / "resources" / f"{resource_id}.yaml")
    except Exception as e:
        if background:
            if done_file:
                with open(done_file, "w") as fp:
                    fp.write("ERROR: \n")
                    for arg in e.args:
                        fp.write(str(arg) + "\n")
        else:
            raise e
    else:
        if done_file:
            with open(done_file, "w") as fp:
                fp.write("success")
        else:
            logger.info(f"success, added {resource_id}")
        repo.commit_all(msg=f"add {resource_id}")


def create(config: Env):
    # Steps to setup the folder and git structure
    main_dir = Path(config.base_path) / "config"
    repo = GitRepo(main_dir)
    resource_dir = main_dir / "resources"

    if not main_dir.is_dir():
        main_dir.mkdir()
        repo.init()

    field_config = main_dir / "fields.yaml"
    field_config.touch()

    main_config = main_dir / "config.yaml"
    main_config.touch()

    if not resource_dir.is_dir():
        resource_dir.mkdir()

    # we auto-commit all changes in the config directory, but we don't want incoming data
    ignore_file = main_dir / ".gitignore"
    if not ignore_file.exists():
        with open(ignore_file, "w") as fp:
            fp.write("incoming")
    incoming_dir = main_dir / "incoming"
    if not incoming_dir.is_dir():
        incoming_dir.mkdir()

    return main_dir, repo


def _get_iterable(resource_obj, key) -> Iterable:
    tags = resource_obj.get(key, ())
    if isinstance(tags, Iterable):
        return tags
    return ()


def _add_tags(
    config_obj: dict[str, object],
    resource_obj: dict[str, object],
    karps_config: dict[str, Any],
    fp_out: TextIOWrapper,
) -> None:
    """
    Takes a  resource-config file and updates Karp-S backend configuration file if needed.
    """
    current_tags = config_obj.get("tags", {})
    for tag in _get_iterable(resource_obj, "tags"):
        if tag not in current_tags:
            if not isinstance(current_tags, dict):
                # TODO do this better, make config_obj into a dataclass
                raise Exception("wrong format for tags in config.yaml")
            if not current_tags:
                config_obj["tags"] = {}
                current_tags = config_obj["tags"]
            current_tags[tag] = karps_config["tags_description"][tag]
    yaml.dump(config_obj, fp_out)


def _read(filename: Path) -> dict[str, object]:
    logger.info(f"Reading input file: {filename}")
    with open(filename) as fp:
        config = yaml.load(fp)
        return config or {}


def _update_config(config_filename: Path, resource_filename: Path, global_filename: Path) -> str:
    # read the input yaml files
    config_obj = _read(config_filename)
    resource_obj = _read(resource_filename)
    karps_config = _read(global_filename)
    # open config.yaml for writing
    with open(config_filename, "w") as fp_out:
        _add_tags(config_obj, resource_obj, karps_config, fp_out)
    return cast(str, resource_obj["resource_id"])


def _update_fields(resource_id: str, backend_fields_file: Path, new_fields_file: Path):
    """
    when running, fields.yaml are created with information about the
    fields that are not already present in the backend. Take this file
    and merge it with <export.karps.output_config_dir>/fields.yaml
    There should be no conflicts.
    """

    # first check the current backend config for fields
    if not backend_fields_file.exists():
        current_fields = []
    else:
        with open(backend_fields_file) as fp:
            current_fields = yaml.load_array(fp) or []
    field_lookup = {field["name"]: field for field in current_fields}
    new_fields = []
    with open(new_fields_file) as fp:
        fields = yaml.load_array(fp)
        for new_field in fields:
            new_label = new_field.get("label")
            if new_field["name"] in field_lookup:
                # update resource list
                field_resources = field_lookup[new_field["name"]]["resource_id"]
                if isinstance(field_resources, list):  # this is for typechecking
                    field_resources.append(resource_id)
                    field_resources = list(set(field_resources))
                    field_lookup[new_field["name"]]["resource_id"] = field_resources
                if field_resources == [resource_id]:
                    # if the field is used only by current resource, allow overwrites
                    field_lookup[new_field["name"]].update(new_field)
                else:
                    # no changes to other resources are allowed
                    if (
                        new_field["type"] != field_lookup[new_field["name"]]["type"]
                        or new_field.get("collection", False)
                        != field_lookup[new_field["name"]].get("collection", False)
                        or (new_label and new_label != field_lookup[new_field["name"]].get("label"))
                    ):
                        raise ValueError(
                            f"There already exists a field called {new_field['name']} with different settings"
                        )
            else:
                new_field["resource_id"] = [resource_id]
                new_fields.append(new_field)

    current_fields.extend(new_fields)

    with open(backend_fields_file, "w") as fp:
        yaml.dump(current_fields, fp)
