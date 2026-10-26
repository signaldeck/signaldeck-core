from __future__ import annotations
from dataclasses import dataclass
from typing import List
from ..models.group import Group  # adjust import if needed

@dataclass(frozen=True)
class UiAssets:
    js: list[str]
    css: list[str]

class UiAssetService:
    """
    Aggregates JS/CSS assets needed for a given set of groups.
    The first-seen order matters because scripts can depend on earlier scripts.
    """
    def get_js_css_for_groups(self, groups: List[Group]) -> UiAssets:
        jsRes: dict[str, None] = {}
        cssRes: dict[str, None] = {}
        for g in groups:
            for a in g.actions:
                jsFiles,cssFiles = a.processor.getAdditionalJsAndCssFiles(a.value)
                for js in jsFiles:
                    jsRes[js] = None
                for css in cssFiles:
                    cssRes[css] = None

        return UiAssets(js=list(jsRes), css=list(cssRes))
