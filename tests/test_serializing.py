from contextlib import contextmanager
import glob
import multiprocessing
from time import sleep
import requests
import uvicorn
import yaml

from karps.api import app


def run_app():
    uvicorn.run(app, host="127.0.0.1", port=12345)


@contextmanager
def start_backend():
    process = multiprocessing.Process(target=run_app)
    try:
        process.start()
        sleep(0.1)
        yield
    finally:
        process.terminate()
        process.join()


def test_casing():
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
        response = requests.get("http://localhost:12345/config")
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
        response = requests.get("http://localhost:12345/search")
        general_check(response.json())

    def test_count():
        response = requests.get("http://localhost:12345/count")
        general_check(response.json())

    with start_backend():
        test_config()
        test_search()
        test_count()
