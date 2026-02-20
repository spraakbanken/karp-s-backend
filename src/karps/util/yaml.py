from typing import Any
import yaml


class IndentDumper(yaml.SafeDumper):
    """Customized YAML dumper that indents lists."""

    def increase_indent(self, flow: bool = False, indentless: bool = False) -> None:
        """Force indentation."""
        return super().increase_indent(flow)


def dump(obj: object, fp, indent: int = 2):
    out = yaml.dump(
        obj, allow_unicode=True, Dumper=IndentDumper, indent=indent, default_flow_style=False, sort_keys=False
    )
    fp.write(out)


def load(fp) -> dict[str, Any]:
    return yaml.safe_load(fp)


def load_array(fp) -> list[dict[str, Any]]:
    return yaml.safe_load(fp)
