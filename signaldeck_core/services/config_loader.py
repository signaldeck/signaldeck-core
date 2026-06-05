# signaldeck_core/services/config_loader.py
import json
from dataclasses import dataclass
from typing import Any

@dataclass(frozen=True)
class AppConfig:
    raw: dict[str, Any]

    @property
    def page_title(self) -> str:
        return self.raw.get("page_title", "Home Control")

    @property
    def processors(self) -> list[dict]:
        return self.raw["processors"]

    @property
    def groups(self) -> list[dict]:
        return self.raw["groups"]

    @property
    def cmd_config(self) -> dict:
        return self.raw.get("cmd", {})

    @property
    def data_stores(self) -> list[dict]:
        return self.raw.get("data_stores", [])

    @property
    def i18n_lang(self) -> str:
        return self.raw.get("i18n", {}).get("lang", "en")

    @property
    def i18n_fallback(self) -> str:
        return self.raw.get("i18n", {}).get("lang_fallback", "en")


class ConfigLoader:
    def load(self, path: str) -> AppConfig:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            processors_from_include=[]
            for processor in data.get("processors", []):
                if "include" in processor:
                    included_path = processor["include"]
                    with open(included_path, "r", encoding="utf-8") as inc_f:
                        included_data = json.load(inc_f)
                        processors_from_include.extend(included_data)
            processor_names = set(p["name"] for p in data.get("processors", []) if "include" not in p)
            data["processors"] = [p for p in data.get("processors", []) if "include" not in p] + [p for p in processors_from_include if p["name"] not in processor_names]
        return AppConfig(raw=data)