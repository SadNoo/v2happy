import unittest

from sspanel_vmess_backend.config import (
    AppConfig,
    DatabaseConfig,
    NodeConfig,
    PanelConfig,
    PluginOptions,
    RuntimeConfig,
    V2RayConfig,
)
from sspanel_vmess_backend.plugin_config import build_v2ray_plugin_config


class PluginConfigTests(unittest.TestCase):
    def test_builds_sspanel_mysql_config(self):
        config = AppConfig(
            database=DatabaseConfig(
                host="db.example.com",
                port=3306,
                user="sspanel_node",
                password="secret",
                database="sspanel",
            ),
            node=NodeConfig(id=121, check_rate=60, speedtest_check_rate=0),
            panel=PanelConfig(key="panel-key", mu_suffix="microsoft.com"),
            v2ray=V2RayConfig(config_path="/etc/v2ray/config.json"),
            plugin=PluginOptions(),
            runtime=RuntimeConfig(dry_run=True, print_config=False),
        )
        payload = build_v2ray_plugin_config(config)
        self.assertEqual(payload["inbounds"], [])
        self.assertEqual(payload["sspanel"]["nodeid"], 121)
        self.assertEqual(payload["sspanel"]["checkRate"], 60)
        self.assertEqual(payload["sspanel"]["SpeedTestCheckRate"], 0)
        self.assertEqual(payload["sspanel"]["usemysql"], 1)
        self.assertEqual(payload["sspanel"]["mysql"]["host"], "db.example.com")
        self.assertEqual(payload["sspanel"]["mysql"]["dbname"], "sspanel")

    def test_contains_reference_outbound_tags(self):
        config = AppConfig(
            database=DatabaseConfig(
                host="127.0.0.1",
                port=3306,
                user="u",
                password="p",
                database="sspanel",
            ),
            node=NodeConfig(id=1),
            panel=PanelConfig(),
            v2ray=V2RayConfig(),
            plugin=PluginOptions(),
            runtime=RuntimeConfig(),
        )
        payload = build_v2ray_plugin_config(config)
        tags = {item.get("tag") for item in payload["outbounds"]}
        self.assertIn("blocked", tags)
        self.assertIn("blockedNodeUserLimited", tags)
        self.assertIn("blockedrule", tags)
        self.assertIn("blockedip", tags)
        self.assertIn("block", tags)


if __name__ == "__main__":
    unittest.main()
