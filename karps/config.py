from dataclasses import dataclass
import json
from typing import Iterator, Optional
from environs import Env
import glob

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


class ResourceConfig(BaseModel):
    resource_id: str
    fields: list[Field]
    # will be str | MultiLangLabel
    label: str

    def format_hit(self, hit):
        def fmt():
            for field, val in zip(self.fields, hit[:-1]):
                if field.collection:
                    if val is not None:
                        val = json.loads(val)
                yield field.name, val

        return dict(fmt())


def get_resource_configs(resource_id: str | None = None) -> Iterator[ResourceConfig]:
    if resource_id:
        glob_pattern = f"{resource_id}.yaml"
    else:
        glob_pattern = "*"
    for resource in glob.glob(f"resources/{glob_pattern}"):
        with open(resource) as fp:
            yield ResourceConfig(**yaml.safe_load(fp))


def get_resource_config(resource_id) -> ResourceConfig:
    return next(get_resource_configs(resource_id))
