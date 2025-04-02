from dataclasses import dataclass
import json
from typing import Iterator, Optional
from environs import Env
import glob

from pydantic import ConfigDict, RootModel
import yaml

from karps.models import BaseModel


@dataclass
class Config:
    host: str
    user: str
    password: str
    database: str


def get_config():
    env = Env()
    env.read_env()

    return Config(
        host=env.str("DB_HOST"),
        user=env.str("DB_USER"),
        password=env.str("DB_PASSWORD"),
        database=env.str("DB_DATABASE"),
    )


class Field(BaseModel):
    name: str
    type: str
    collection: Optional[bool] = False


class MultiLang(RootModel[str | dict[str, str]]): ...


class ResourceConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    resource_id: str
    fields: list[Field]
    label: MultiLang
    description: MultiLang | None = None
    word: str
    word_description: MultiLang
    updated: int
    size: int
    link: str
    tags: list[str] | None = None

    def format_hit(self, hit):
        def fmt():
            for field, val in zip(self.fields, hit[:-1]):
                if field.collection:
                    if val is not None:
                        val = json.loads(val)
                yield field.name, val

        return dict(fmt())


class Tag(BaseModel):
    name: MultiLang
    description: MultiLang


class ConfigResponse(BaseModel):
    resources: list[ResourceConfig]
    tags: dict[str, Tag]


class MainConfig(BaseModel):
    tags: dict[str, Tag]


def get_resource_configs(resource_id: str | None = None) -> Iterator[ResourceConfig]:
    if resource_id:
        glob_pattern = f"{resource_id}.yaml"
    else:
        glob_pattern = "*"
    for resource in glob.glob(f"config/resources/{glob_pattern}"):
        with open(resource) as fp:
            yield ResourceConfig(**yaml.safe_load(fp))


def get_tags() -> dict[str, Tag]:
    with open("config/config.yaml") as fp:
        return MainConfig(**yaml.safe_load(fp)).tags


def get_resource_config(resource_id) -> ResourceConfig:
    return next(get_resource_configs(resource_id))
