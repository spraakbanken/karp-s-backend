from contextlib import contextmanager
from dataclasses import dataclass
import json
import os
from typing import Iterable, Iterator, Optional
import environs
import glob

from karps.errors import errors
from pydantic import ConfigDict, RootModel
import yaml

from karps.models import BaseModel


@dataclass
class Env:
    host: str
    user: str
    password: str
    database: str
    base_path: str = ""


def get_env() -> Env:
    env = environs.Env()
    env.read_env()

    return Env(
        host=env.str("DB_HOST"),
        user=env.str("DB_USER"),
        password=env.str("DB_PASSWORD"),
        database=env.str("DB_DATABASE"),
        base_path=env.str("BASE_PATH", ""),
    )


class MultiLang(RootModel[str | dict[str, str]]): ...


class Field(BaseModel):
    name: str
    type: str
    collection: Optional[bool] = False
    label: MultiLang | None = None

    def model_post_init(self, _):
        if self.label is None:
            self.label = MultiLang(self.name)


class ResourceConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    resource_id: str
    fields: list[str]
    label: MultiLang
    description: MultiLang | None = None
    word: str
    word_description: MultiLang
    updated: int
    size: int
    link: str
    tags: list[str] | None = None


class Tag(BaseModel):
    label: MultiLang
    description: MultiLang


class ConfigResponse(BaseModel):
    resources: list[ResourceConfig]
    tags: dict[str, Tag]
    fields: dict[str, Field]


class MainConfig(BaseModel):
    tags: dict[str, Tag]
    fields: dict[str, Field]


@contextmanager
def open_local(config: Env, path):
    fp = None
    try:
        config.base_path
        fp = open(os.path.join(config.base_path or ".", path))
        yield fp
    finally:
        if fp:
            fp.close()


def get_resource_configs(config: Env, resource_id: str | None = None) -> Iterator[ResourceConfig]:
    if resource_id:
        glob_pattern = f"{resource_id}.yaml"
    else:
        glob_pattern = "*"
    for resource in glob.glob(os.path.join(config.base_path, f"config/resources/{glob_pattern}")):
        with open_local(config, resource) as fp:
            yield ResourceConfig(**yaml.safe_load(fp))


def get_resource_config(env: Env, resource_id) -> ResourceConfig | None:
    try:
        return next(get_resource_configs(env, resource_id))
    except StopIteration:
        raise errors.UserError("One or more of the resources are missing")


def load_config(env: Env) -> MainConfig:
    with open_local(env, "config/config.yaml") as fp:
        main = yaml.safe_load(fp)
    with open_local(env, "config/fields.yaml") as fp:
        fields = yaml.safe_load(fp)
    main["fields"] = {field["name"]: field for field in fields}
    return MainConfig(**main)


def format_hit(main_config: MainConfig, resource_config: ResourceConfig, hit) -> dict[str, object]:
    field_lookup = main_config.fields

    def fmt():
        for field_name, val in zip(resource_config.fields, hit[:-1]):
            field = field_lookup[field_name]
            if field.collection:
                if val is not None:
                    val = json.loads(val)
            yield field.name, val

    return dict(fmt())


def ensure_fields_exist(resources: list[ResourceConfig], fields: Iterable[str]):
    for resource in resources:
        for field in fields:
            if field not in ("resource_id", "word") and field not in resource.fields:
                raise errors.UserError(f"{field} does not exist in {resource.resource_id}")
