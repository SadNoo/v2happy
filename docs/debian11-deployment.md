# Debian 11 部署说明：SSPanel-Uim VMess 单端口后端

这是一个数据库直连模式的 V2Ray / VMess 单端口后端。它不新增面板 API，只读写 SSPanel-Uim 已有数据库表。

## 系统要求

- Debian 11 及以上 Linux
- Python 3.9+
- MySQL/MariaDB 可访问
- Xray-core 或兼容 V2Ray-core

Debian 11 默认 Python 3.9 可用。本项目使用 `tomli` 兼容 Python 3.9/3.10，Python 3.11+ 会使用标准库 `tomllib`。

## 面板数据库表

读取：

- `ss_node`
- `user`

写入：

- `user.u`
- `user.d`
- `user.t`
- `user_traffic_log`
- `ss_node.node_heartbeat`
- `ss_node.node_bandwidth`
- `ss_node_online_log`
- `ss_node_info`
- `alive_ip`

不会调用或新增面板 API。

## 端口规则

后端监听端口严格按面板 `Tools::v2Array($node->server)` 的最终 `port`：

```text
第 2 段为空或 0 -> port = 443
第 2 段非空 -> port = 第 2 段
第 6 段存在 outside_port -> port = outside_port
```

因此：

```text
listen_port = Tools::v2Array(ss_node.server)["port"]
```

## 安装

```bash
cd /opt
python3 -m venv /opt/sspanel-vmess-single-port-backend/.venv
/opt/sspanel-vmess-single-port-backend/.venv/bin/pip install -U pip
/opt/sspanel-vmess-single-port-backend/.venv/bin/pip install /opt/sspanel-vmess-single-port-backend
```

配置：

```bash
mkdir -p /etc/sspanel-vmess-backend /var/lib/sspanel-vmess
cp config.example.toml /etc/sspanel-vmess-backend/config.toml
```

编辑：

```bash
nano /etc/sspanel-vmess-backend/config.toml
```

systemd：

```bash
cp systemd/sspanel-vmess-backend.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now sspanel-vmess-backend
```

## 运行

单次 dry-run：

```bash
sspanel-vmess-backend -c config.example.toml --log-level DEBUG
```

常驻模式由配置控制：

```toml
[runtime]
dry_run = false
run_once = false
```

## 当前实现范围

已实现：

- 数据库直连配置
- `ss_node` 节点读取
- `user` 用户读取和过滤
- 面板 `Tools::v2Array()` 等价解析
- 面板 UUID 等价生成
- Xray VMess inbound 配置生成
- Xray 配置文件写入和 reload/restart 命令
- Xray stats 命令输出解析
- stats 游标，避免重复计费
- access log cursor
- access log 中 `sspanel-user-{id}` 和客户端 IP 解析
- 流量、在线 IP、节点状态写库
- `disconnect_ip`、`forbidden_ip`、`forbidden_port` 转换为 Xray routing block 规则

仍需在真实 Debian 11 节点上验证：

- Xray `api statsquery` 命令输出格式
- Xray access log 是否稳定包含 `sspanel-user-{id}`
- reload 命令对应的 systemd 服务名

## 注意

不要同时启用 WebAPI 后端和这个 DB 后端写同一个节点，否则流量会重复计费。
