import re
import unittest
from pathlib import Path


class DocumentationHomepageTest(unittest.TestCase):
    def test_homepage_is_fully_bilingual(self) -> None:
        homepage = Path("docs/index.md").read_text(encoding="utf-8")
        headings = [line for line in homepage.splitlines() if line.startswith("#")]

        self.assertTrue(headings)
        self.assertTrue(all(" / " in heading for heading in headings))
        self.assertIn("[Chinese edition / 中文版]", homepage)
        self.assertGreaterEqual(len(re.findall(r"[\u4e00-\u9fff]", homepage)), 120)

    def test_homepage_readmes_and_navigation_expose_three_learning_modes(self) -> None:
        homepage = Path("docs/index.md").read_text()
        english = Path("README.md").read_text()
        chinese = Path("README.zh-CN.md").read_text()
        navigation = Path("mkdocs.yml").read_text()

        for name in ("Mechanism Tutorial", "Self-Guided Rebuild", "Agent-Guided Rebuild"):
            self.assertIn(name, english)
            self.assertIn(name, navigation)
        for name in ("机制教程", "自主重建", "Agent 带教"):
            self.assertIn(name, chinese)
            self.assertIn(name, navigation)
        self.assertIn("Mechanism Tutorial / 机制教程", homepage)
        self.assertIn("Self-Guided Rebuild / 自主重建", homepage)
        self.assertIn("Agent-Guided Rebuild / Agent 带教", homepage)

    def test_agent_pages_are_short_usage_guides(self) -> None:
        for path in (Path("docs/agent-guided.md"), Path("docs/zh/agent-guided.md")):
            page = path.read_text()
            self.assertIn("开始 Agent 带教 Stage 03", page)
            for internal in ("build_journey.py agent", ".journey/", "agent-only", "branch"):
                self.assertNotIn(internal, page)

    def test_agent_contract_and_language_switch_are_installed(self) -> None:
        contract = Path("AGENTS.md").read_text()
        self.assertIn("1 through 10", contract)
        self.assertIn("python -m journey.tools.build_journey agent NN", contract)
        self.assertIn("Never create or switch a teaching branch", contract)
        navigation = Path("mkdocs.yml").read_text()
        switch = Path("docs/assets/javascripts/language-switch.js").read_text()
        self.assertIn("assets/javascripts/language-switch.js", navigation)
        self.assertIn('"journey/"', switch)

    def test_generated_journey_uses_collapsed_deliverables(self) -> None:
        for root, label, heading in (
            (Path("docs/journey"), '??? note "Deliverable files"', "### Deliverable files"),
            (Path("docs/zh/journey"), '??? note "交付文件"', "### 交付文件"),
        ):
            stages = sorted(root.glob("stage-*.md"))
            self.assertEqual(len(stages), 10)
            for stage in stages:
                lesson = stage.read_text()
                self.assertIn(label, lesson)
                self.assertNotIn(heading, lesson)


if __name__ == "__main__":
    unittest.main()
