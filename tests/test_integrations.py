import json
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RELAY_MCP_URL = "https://hermes-together-render-relay.onrender.com/mcp"


class CrossCliIntegrationTests(unittest.TestCase):
    def test_codex_plugin_is_discoverable_and_contains_the_shared_mcp_tools(self):
        marketplace_path = ROOT / ".agents" / "plugins" / "marketplace.json"
        marketplace = json.loads(marketplace_path.read_text(encoding="utf-8"))
        plugin_entry = next(item for item in marketplace["plugins"] if item["name"] == "hermes-together")
        source_path = plugin_entry["source"]["path"]
        plugin_root = (ROOT / source_path).resolve()
        manifest = json.loads((plugin_root / "plugin.json").read_text(encoding="utf-8"))
        mcp = json.loads((plugin_root / "mcp.json").read_text(encoding="utf-8"))

        self.assertEqual(manifest["name"], "hermes-together")
        self.assertEqual(mcp["mcpServers"]["hermes-together"]["type"], "streamable-http")
        self.assertEqual(mcp["mcpServers"]["hermes-together"]["url"], RELAY_MCP_URL)
        self.assertTrue((plugin_root / "skills" / "hermes-together" / "SKILL.md").is_file())

    def test_opencode_config_and_command_target_the_same_room_workflow(self):
        config = json.loads((ROOT / "integrations" / "opencode" / "opencode.json").read_text(encoding="utf-8"))
        server = config["mcp"]["hermes-together"]
        self.assertEqual(server["type"], "remote")
        self.assertEqual(server["url"], RELAY_MCP_URL)
        self.assertTrue(server["enabled"])

        command = (ROOT / "integrations" / "opencode" / "commands" / "together-online.md").read_text(encoding="utf-8")
        self.assertIn("$ARGUMENTS", command)
        self.assertIn("together_join", command)
        self.assertIn("room_code", command)
        self.assertIn("together_agents", command)


if __name__ == "__main__":
    unittest.main()
