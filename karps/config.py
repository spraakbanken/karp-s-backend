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


def get_resource_configs(resource_id: str | None = None) -> Iterator[ResourceConfig]:
    if resource_id:
        glob_pattern = f"{resource_id}.yaml"
    else:
        glob_pattern = "*"
    for resource in glob.glob(f"config/resources/{glob_pattern}"):
        with open(resource) as fp:
            yield ResourceConfig(**yaml.safe_load(fp))


def get_resource_config(resource_id) -> ResourceConfig:
    return next(get_resource_configs(resource_id))


def load_config() -> MainConfig:
    with open("config/config.yaml") as fp:
        main = yaml.safe_load(fp)
    with open("config/fields.yaml") as fp:
        fields = yaml.safe_load(fp)
    main["fields"] = {field["name"]: field for field in fields}
    return MainConfig(**main)


def format_hit(main_config: MainConfig, resource_config: ResourceConfig, hit):
    field_lookup = main_config.fields

    def fmt():
        for field_name, val in zip(resource_config.fields, hit[:-1]):
            field = field_lookup[field_name]
            if field.collection:
                if val is not None:
                    val = json.loads(val)
            yield field.name, val

    return dict(fmt())
