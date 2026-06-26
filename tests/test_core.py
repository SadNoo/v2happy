import json
import unittest

from sspanel_vmess_backend.accesslog import parse_access_line
from sspanel_vmess_backend.models import User
from sspanel_vmess_backend.v2ray import build_xray_config, user_uuid, v2_array
from sspanel_vmess_backend.xray import parse_stats_output


class V2ArrayTests(unittest.TestCase):
    def test_default_port(self):
        endpoint = v2_array("example.com;0;0;tcp")
        self.assertEqual(endpoint.port, 443)
        self.assertEqual(endpoint.net, "tcp")

    def test_outside_port_is_final_port(self):
        endpoint = v2_array("origin.example.com;10086;0;ws;none;server=cdn.example.com|outside_port=443|path=/ray|host=cdn.example.com")
        self.assertEqual(endpoint.add, "cdn.example.com")
        self.assertEqual(endpoint.port, 443)
        self.assertEqual(endpoint.net, "ws")
        self.assertEqual(endpoint.path, "/ray")
        self.assertEqual(endpoint.host, "cdn.example.com")

    def test_uuid_matches_panel_rule(self):
        self.assertEqual(user_uuid(1, "passwd"), "d7998791-c432-3ea8-85f7-dc39b1a94f69")


class XrayConfigTests(unittest.TestCase):
    def test_builds_vmess_clients(self):
        endpoint = v2_array("example.com;443;0;ws;none;path=/ray|host=example.com")
        user = User(
            id=123,
            email="user@example.com",
            port=10000,
            passwd="pw",
            forbidden_ip="",
            forbidden_port="",
            disconnect_ip="",
            node_speedlimit=0.0,
            u=0,
            d=0,
            transfer_enable=1024,
            enable=1,
            uuid=user_uuid(123, "pw"),
            vmess_email="sspanel-user-123",
        )
        config = build_xray_config(
            endpoint,
            [user],
            "0.0.0.0",
            "127.0.0.1:10085",
            "/var/log/xray/access.log",
            "/var/log/xray/error.log",
        )
        inbound = config["inbounds"][0]
        self.assertEqual(inbound["port"], 443)
        self.assertEqual(inbound["settings"]["clients"][0]["email"], "sspanel-user-123")
        self.assertEqual(inbound["streamSettings"]["wsSettings"]["path"], "/ray")

    def test_builds_user_block_rules(self):
        endpoint = v2_array("example.com;443;0;tcp")
        user = User(
            id=123,
            email="user@example.com",
            port=10000,
            passwd="pw",
            forbidden_ip="10.0.0.0/8",
            forbidden_port="25,465",
            disconnect_ip="203.0.113.10",
            node_speedlimit=0.0,
            u=0,
            d=0,
            transfer_enable=1024,
            enable=1,
            uuid=user_uuid(123, "pw"),
            vmess_email="sspanel-user-123",
        )
        config = build_xray_config(
            endpoint,
            [user],
            "0.0.0.0",
            "127.0.0.1:10085",
            "/var/log/xray/access.log",
            "/var/log/xray/error.log",
        )
        rules = config["routing"]["rules"]
        self.assertEqual(rules[0]["source"], ["203.0.113.10"])
        self.assertEqual(rules[1]["ip"], ["10.0.0.0/8"])
        self.assertEqual(rules[2]["port"], "25,465")


class StatsTests(unittest.TestCase):
    def test_parse_json_stats(self):
        output = json.dumps(
            {
                "stat": [
                    {"name": "user>>>sspanel-user-1>>>traffic>>>uplink", "value": 10},
                    {"name": "user>>>sspanel-user-1>>>traffic>>>downlink", "value": 20},
                ]
            }
        )
        stats = parse_stats_output(output)
        self.assertEqual(stats["user>>>sspanel-user-1>>>traffic>>>uplink"], 10)
        self.assertEqual(stats["user>>>sspanel-user-1>>>traffic>>>downlink"], 20)

    def test_parse_text_stats(self):
        output = "user>>>sspanel-user-1>>>traffic>>>uplink: 10\n"
        stats = parse_stats_output(output)
        self.assertEqual(stats["user>>>sspanel-user-1>>>traffic>>>uplink"], 10)


class AccessLogTests(unittest.TestCase):
    def test_parse_line_with_user_and_ip(self):
        parsed = parse_access_line("2026/06/26 from 203.0.113.10:1234 accepted tcp:example.com:443 email: sspanel-user-9")
        self.assertEqual(parsed, (9, "203.0.113.10"))

    def test_ignore_line_without_user(self):
        self.assertIsNone(parse_access_line("2026/06/26 from 203.0.113.10:1234 accepted"))


if __name__ == "__main__":
    unittest.main()
