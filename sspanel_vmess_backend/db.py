from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator, Optional, Sequence

from .config import DatabaseConfig
from .models import AliveIp, NodeInfo, TrafficDelta, User, clean_text, to_float, to_int
from .v2ray import user_uuid, vmess_email


class Database:
    def __init__(self, config: DatabaseConfig) -> None:
        self.config = config

    @contextmanager
    def connect(self) -> Iterator[object]:
        import pymysql
        from pymysql.cursors import DictCursor

        kwargs = {
            "host": self.config.host,
            "port": self.config.port,
            "user": self.config.user,
            "password": self.config.password,
            "database": self.config.database,
            "charset": self.config.charset,
            "cursorclass": DictCursor,
            "autocommit": False,
        }
        if self.config.ssl_enable:
            kwargs["ssl"] = {
                "ca": self.config.ssl_ca,
                "cert": self.config.ssl_cert,
                "key": self.config.ssl_key,
            }

        conn = pymysql.connect(**kwargs)
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def fetch_node(self, node_id: int) -> Optional[NodeInfo]:
        sql = """
            SELECT
              node_group,
              node_class,
              node_speedlimit,
              traffic_rate,
              mu_only,
              sort,
              server,
              node_bandwidth,
              node_bandwidth_limit
            FROM ss_node
            WHERE id = %s
              AND (node_bandwidth < node_bandwidth_limit OR node_bandwidth_limit = 0)
        """
        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (node_id,))
                row = cursor.fetchone()
        if row is None:
            return None
        return NodeInfo(
            node_id=node_id,
            node_group=to_int(row["node_group"]),
            node_class=to_int(row["node_class"]),
            node_speedlimit=to_float(row["node_speedlimit"]),
            traffic_rate=to_float(row["traffic_rate"], 1.0),
            mu_only=to_int(row["mu_only"]),
            sort=to_int(row["sort"]),
            server=clean_text(row["server"]),
            node_bandwidth=to_int(row["node_bandwidth"]),
            node_bandwidth_limit=to_int(row["node_bandwidth_limit"]),
        )

    def fetch_users(self, node: NodeInfo) -> list[User]:
        fields = """
            id,
            email,
            port,
            passwd,
            forbidden_ip,
            forbidden_port,
            disconnect_ip,
            node_speedlimit,
            u,
            d,
            transfer_enable,
            enable
        """
        if node.node_group == 0:
            sql = f"""
                SELECT {fields}
                FROM user
                WHERE ((class >= %s) OR is_admin = 1)
                  AND enable = 1
                  AND expire_in > NOW()
                  AND transfer_enable > u + d
            """
            params: tuple[object, ...] = (node.node_class,)
        else:
            sql = f"""
                SELECT {fields}
                FROM user
                WHERE ((class >= %s AND node_group = %s) OR is_admin = 1)
                  AND enable = 1
                  AND expire_in > NOW()
                  AND transfer_enable > u + d
            """
            params = (node.node_class, node.node_group)

        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                rows = cursor.fetchall()

        users: list[User] = []
        for row in rows:
            user_id = to_int(row["id"])
            passwd = clean_text(row["passwd"])
            users.append(
                User(
                    id=user_id,
                    email=clean_text(row["email"]),
                    port=to_int(row["port"]),
                    passwd=passwd,
                    forbidden_ip=clean_text(row["forbidden_ip"]),
                    forbidden_port=clean_text(row["forbidden_port"]),
                    disconnect_ip=clean_text(row["disconnect_ip"]),
                    node_speedlimit=to_float(row["node_speedlimit"]),
                    u=to_int(row["u"]),
                    d=to_int(row["d"]),
                    transfer_enable=to_int(row["transfer_enable"]),
                    enable=to_int(row["enable"]),
                    uuid=user_uuid(user_id, passwd),
                    vmess_email=vmess_email(user_id),
                )
            )
        return users

    def write_cycle(
        self,
        node: NodeInfo,
        traffic: Sequence[TrafficDelta],
        alive_ips: Sequence[AliveIp],
        online_user_count: int,
        uptime: int,
        load: str,
    ) -> None:
        with self.connect() as conn:
            with conn.cursor() as cursor:
                raw_bandwidth = 0
                for item in traffic:
                    billed_u = int(item.upload * node.traffic_rate)
                    billed_d = int(item.download * node.traffic_rate)
                    raw_bandwidth += item.total
                    cursor.execute(
                        """
                        UPDATE user
                        SET u = u + %s,
                            d = d + %s,
                            t = UNIX_TIMESTAMP()
                        WHERE id = %s
                        """,
                        (billed_u, billed_d, item.user_id),
                    )
                    cursor.execute(
                        """
                        INSERT INTO user_traffic_log
                          (user_id, u, d, node_id, rate, traffic, log_time)
                        VALUES
                          (%s, %s, %s, %s, %s, %s, UNIX_TIMESTAMP())
                        """,
                        (
                            item.user_id,
                            item.upload,
                            item.download,
                            node.node_id,
                            node.traffic_rate,
                            flow_auto_show(int(item.total * node.traffic_rate)),
                        ),
                    )

                cursor.execute(
                    """
                    UPDATE ss_node
                    SET node_heartbeat = UNIX_TIMESTAMP(),
                        node_bandwidth = node_bandwidth + %s
                    WHERE id = %s
                    """,
                    (raw_bandwidth, node.node_id),
                )

                cursor.execute(
                    """
                    INSERT INTO ss_node_online_log
                      (node_id, online_user, log_time)
                    VALUES
                      (%s, %s, UNIX_TIMESTAMP())
                    """,
                    (node.node_id, online_user_count),
                )

                cursor.execute(
                    """
                    INSERT INTO ss_node_info
                      (node_id, uptime, `load`, log_time)
                    VALUES
                      (%s, %s, %s, UNIX_TIMESTAMP())
                    """,
                    (node.node_id, uptime, load),
                )

                for item in alive_ips:
                    cursor.execute(
                        """
                        INSERT INTO alive_ip
                          (nodeid, userid, ip, datetime)
                        VALUES
                          (%s, %s, %s, UNIX_TIMESTAMP())
                        """,
                        (node.node_id, item.user_id, item.ip),
                    )


def flow_auto_show(value: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    amount = float(value)
    unit = units[0]
    for unit in units:
        if amount < 1024 or unit == units[-1]:
            break
        amount /= 1024
    return f"{amount:.2f}{unit}"
