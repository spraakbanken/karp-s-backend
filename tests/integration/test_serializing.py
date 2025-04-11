import glob
import yaml

from tests.integration.conftest import Backend


def test_casing(backend: Backend):
    """test that all field names, resource ids and tag names are untouched, but that all other keys are camel-case, including resourceId"""
    with open("config/config.yaml") as fp:
        main = yaml.safe_load(fp)
    with open("config/fields.yaml") as fp:
        fields = yaml.safe_load(fp)
    resource_ids = []
    for resource in glob.glob("config/resources/*"):
        with open(resource) as fp:
            resource_config = yaml.safe_load(fp)
            resource_ids.append(resource_config["resource_id"])

    def test_config():
        response = backend.get("/config")
        config = response.json()
        for key in config.keys():
            assert "_" not in key
        for field in fields:
            name = field["name"]
            assert name in config["fields"]
            assert config["fields"][name]["name"] == name
            for key in config["fields"][name].keys():
                assert "_" not in key
        for tag_name in main["tags"].keys():
            assert tag_name in config["tags"]
            for key in config["tags"][tag_name].keys():
                assert "_" not in key
        for resource in config["resources"]:
            for key in resource.keys():
                assert "_" not in key

    def general_check(tree):
        for key, value in tree.items():
            if key == "entry":
                continue
            assert "_" not in key
            if isinstance(value, list):
                for inner_val in value:
                    if isinstance(inner_val, dict):
                        general_check(inner_val)
            if isinstance(value, dict):
                general_check(value)

    def test_search():
        response = backend.get("/search")
        general_check(response.json())

    def test_count():
        response = backend.get("/count")
        general_check(response.json())

    test_config()
    test_search()
    test_count()
