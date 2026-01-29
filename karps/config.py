from contextlib import contextmanager
from dataclasses import dataclass
import os
from typing import Iterable, Iterator, Optional
import environs
import glob

from karps.errors import errors
from pydantic import ConfigDict, RootModel, Field as PydanticField
import yaml

from karps.models import BaseModel


@dataclass
class Env:
    host: str
    user: str
    password: str
    database: str
    base_path: str = ""
    logging_dir: str = ""
    sql_query_logging: bool = False


def get_env() -> Env:
    env = environs.Env()
    env.read_env()

    return Env(
        host=env.str("DB_HOST"),
        user=env.str("DB_USER"),
        password=env.str("DB_PASSWORD"),
        database=env.str("DB_DATABASE"),
        base_path=env.str("BASE_PATH", ""),
        logging_dir=env.str("LOGGING_DIR", ""),
        sql_query_logging=env.bool("SQL_QUERY_LOGGING", False),
    )


class MultiLang(RootModel[str | dict[str, str]]): ...


class Field(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str = PydanticField(
        ..., description="(Machine) name of the field. This name is used by resources to list the available fields."
    )
    type: str = PydanticField(..., description="Type of the field, can be text, integer or float or table.")
    collection: Optional[bool] = PydanticField(default=False, description="If `true`, the field is a list of `type`.")
    label: MultiLang | None = PydanticField(
        default=None, description="Label for the field, can be in mulitple languages."
    )
    fields: dict[str, "Field"] = PydanticField(
        default_factory=dict, description="If type is table, then there can be sub-fields (that cannot be table)."
    )
    resource_id: list[str] = PydanticField(
        default_factory=list, description="The resources that this field is available in."
    )

    def model_post_init(self, _):
        if self.label is None:
            self.label = MultiLang(self.name)


class EntryWord(BaseModel):
    field: str
    description: MultiLang


class ResourceField(BaseModel):
    name: str = PydanticField(
        ..., description="The name of the field. Corresponds to a key under the top-level `fields`."
    )
    primary: bool = PydanticField(
        ..., description="Fields with `primary: true` are more relevant than fields with `primary: false`."
    )


class ResourceConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    resource_id: str = PydanticField(..., description="The resource ID")
    fields: list[ResourceField] = PydanticField(..., description="The fields available in this resource.")
    label: MultiLang = PydanticField(..., description="Name for this resource, can be in mulitple languages.")
    description: MultiLang | None = PydanticField(
        default=None, description="Description of this resource, can be in mulitple languages."
    )
    entry_word: EntryWord = PydanticField(..., description="The default field for this resource.")
    updated: int = PydanticField(
        ...,
        description="The timestamp for when this resource was last updated (data or configuration). UNIX timestmap in milliseconds.",
    )
    size: int = PydanticField(..., description="The number of entries in this resource.")
    link: str = PydanticField(..., description="A link to a relevant page for the resource.")
    tags: list[str] | None = PydanticField(
        default=None,
        description="The tags for this resource, see top-level `tags`, for tag labels and description.",
    )

    @property
    def field_names(self):
        return [resource_field.name for resource_field in self.fields]


class Tag(BaseModel):
    label: MultiLang
    description: MultiLang


class ConfigResponse(BaseModel):
    resources: list[ResourceConfig] = PydanticField(..., description="All resources available in this instance.")
    tags: dict[str, Tag] = PydanticField(
        ..., description='All tags available in this instance. Will be used by some of the resources under "resources".'
    )
    fields: dict[str, Field] = PydanticField(..., description="All fields available in this instance.")


class MainConfig(BaseModel):
    tags: dict[str, Tag]
    fields: dict[str, Field]


@contextmanager
def open_local(config: Env, path: str):
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
    # TODO use same sort as in search
    for resource in sorted(glob.glob(os.path.join(config.base_path, f"config/resources/{glob_pattern}"))):
        with open_local(config, resource) as fp:
            yield ResourceConfig(**yaml.safe_load(fp))


def get_resource_config(env: Env, resource_id: str) -> ResourceConfig:
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


def format_hit(
    main_config: MainConfig, resource_config: ResourceConfig, hit: list[str | int | bool | None]
) -> dict[str, object]:
    field_lookup = main_config.fields

    def fmt():
        for resource_field, val in zip(resource_config.fields, hit):
            field = field_lookup[resource_field.name]
            yield field.name, val

    return dict(fmt())


def ensure_fields_exist(resources: list[ResourceConfig], fields: Iterable[str]):
    for resource in resources:
        for field in fields:
            if field not in ("resource_id", "entry_word") and field not in resource.field_names:
                raise errors.UserError(f"{field} does not exist in {resource.resource_id}")


def get_collection_fields(main_config: MainConfig, resources: list[ResourceConfig]) -> Iterable[str]:
    fields: set[str] = set()
    for resource in resources:
        for resource_field in resource.fields:
            if main_config.fields[resource_field.name].collection:
                fields.add(resource_field.name)
    return fields


def get_table_fields(main_config: MainConfig, resources: list[ResourceConfig]) -> dict[str, list[str]]:
    fields: dict[str, list[str]] = {}
    for resource in resources:
        for resource_field in resource.fields:
            field = main_config.fields[resource_field.name]
            if field.type == "table":
                if resource_field.name in fields:
                    raise errors.UserError(
                        f"{field} has different sub-fields in selected resources ({field.fields.keys()} vs. {fields[resource_field.name]})"
                    )
                else:
                    fields[resource_field.name] = list(field.fields.keys())
    return fields
