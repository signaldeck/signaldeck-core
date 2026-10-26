"""Regression tests for stable asset order across groups and processors."""

from types import SimpleNamespace
import unittest

from signaldeck_core.services.ui_asset_service import UiAssetService


class FakeProcessor:
    def __init__(self, js, css):
        self.js = js
        self.css = css

    def getAdditionalJsAndCssFiles(self, value):
        return self.js, self.css


def action(js, css):
    return SimpleNamespace(processor=FakeProcessor(js, css), value=None)


class UiAssetServiceTests(unittest.TestCase):
    def test_scripts_keep_dependency_order_while_duplicates_are_removed(self):
        groups = [
            SimpleNamespace(actions=[
                action(
                    ["markdown-it.min.js", "chat_markdown.js", "chat.js"],
                    ["chat.css", "shared.css"],
                ),
                action(["chat.js", "notifications.js"], ["shared.css"]),
            ]),
            SimpleNamespace(actions=[
                action(["markdown-it.min.js", "widgets.js"], ["widgets.css", "chat.css"]),
            ]),
        ]

        assets = UiAssetService().get_js_css_for_groups(groups)

        self.assertEqual(assets.js, [
            "markdown-it.min.js", "chat_markdown.js", "chat.js",
            "notifications.js", "widgets.js",
        ])
        self.assertEqual(assets.css, ["chat.css", "shared.css", "widgets.css"])

    def test_empty_groups_return_empty_assets(self):
        assets = UiAssetService().get_js_css_for_groups([])
        self.assertEqual(assets.js, [])
        self.assertEqual(assets.css, [])


if __name__ == "__main__":
    unittest.main()
