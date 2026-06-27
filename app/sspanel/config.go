package sspanel

import proto "github.com/golang/protobuf/proto"

type MySQLConfig struct {
	Host     string `protobuf:"bytes,1,opt,name=host,proto3" json:"host,omitempty"`
	Port     uint32 `protobuf:"varint,2,opt,name=port,proto3" json:"port,omitempty"`
	User     string `protobuf:"bytes,3,opt,name=user,proto3" json:"user,omitempty"`
	Password string `protobuf:"bytes,4,opt,name=password,proto3" json:"password,omitempty"`
	DBName   string `protobuf:"bytes,5,opt,name=dbname,proto3" json:"dbname,omitempty"`
}

func (m *MySQLConfig) Reset()         { *m = MySQLConfig{} }
func (m *MySQLConfig) String() string { return proto.CompactTextString(m) }
func (*MySQLConfig) ProtoMessage()    {}

func (m *MySQLConfig) GetHost() string {
	if m != nil {
		return m.Host
	}
	return ""
}

func (m *MySQLConfig) GetPort() uint32 {
	if m != nil && m.Port > 0 {
		return m.Port
	}
	return 3306
}

func (m *MySQLConfig) GetUser() string {
	if m != nil {
		return m.User
	}
	return ""
}

func (m *MySQLConfig) GetPassword() string {
	if m != nil {
		return m.Password
	}
	return ""
}

func (m *MySQLConfig) GetDBName() string {
	if m != nil {
		return m.DBName
	}
	return ""
}

type Config struct {
	NodeId             uint32       `protobuf:"varint,1,opt,name=node_id,json=nodeId,proto3" json:"node_id,omitempty"`
	CheckRate          uint32       `protobuf:"varint,2,opt,name=check_rate,json=checkRate,proto3" json:"check_rate,omitempty"`
	SpeedTestCheckRate uint32       `protobuf:"varint,3,opt,name=speed_test_check_rate,json=speedTestCheckRate,proto3" json:"speed_test_check_rate,omitempty"`
	PanelUrl           string       `protobuf:"bytes,4,opt,name=panel_url,json=panelUrl,proto3" json:"panel_url,omitempty"`
	PanelKey           string       `protobuf:"bytes,5,opt,name=panel_key,json=panelKey,proto3" json:"panel_key,omitempty"`
	DownWithPanel      uint32       `protobuf:"varint,6,opt,name=down_with_panel,json=downWithPanel,proto3" json:"down_with_panel,omitempty"`
	MuRegex            string       `protobuf:"bytes,7,opt,name=mu_regex,json=muRegex,proto3" json:"mu_regex,omitempty"`
	MuSuffix           string       `protobuf:"bytes,8,opt,name=mu_suffix,json=muSuffix,proto3" json:"mu_suffix,omitempty"`
	MySQL              *MySQLConfig `protobuf:"bytes,9,opt,name=mysql,proto3" json:"mysql,omitempty"`
	PanelType          string       `protobuf:"bytes,10,opt,name=paneltype,proto3" json:"paneltype,omitempty"`
	UseMySQL           uint32       `protobuf:"varint,11,opt,name=usemysql,proto3" json:"usemysql,omitempty"`
	CFKey              string       `protobuf:"bytes,12,opt,name=cf_key,json=cfKey,proto3" json:"cf_key,omitempty"`
	CFEmail            string       `protobuf:"bytes,13,opt,name=cf_email,json=cfEmail,proto3" json:"cf_email,omitempty"`
	ProxyTCP           bool         `protobuf:"varint,14,opt,name=proxy_tcp,json=proxyTcp,proto3" json:"proxy_tcp,omitempty"`
	AliKey             string       `protobuf:"bytes,15,opt,name=ali_key,json=aliKey,proto3" json:"ali_key,omitempty"`
	AliSecret          string       `protobuf:"bytes,16,opt,name=ali_secret,json=aliSecret,proto3" json:"ali_secret,omitempty"`
	CacheDurationSec   uint32       `protobuf:"varint,17,opt,name=cache_duration_sec,json=cacheDurationSec,proto3" json:"cache_duration_sec,omitempty"`
	HTMLPath           string       `protobuf:"bytes,18,opt,name=html_path,json=htmlPath,proto3" json:"html_path,omitempty"`
	TrafficReportSec   uint32       `protobuf:"varint,19,opt,name=traffic_report_sec,json=trafficReportSec,proto3" json:"traffic_report_sec,omitempty"`
	AliveIPReportSec   uint32       `protobuf:"varint,20,opt,name=alive_ip_report_sec,json=aliveIpReportSec,proto3" json:"alive_ip_report_sec,omitempty"`
	NodeReportSec      uint32       `protobuf:"varint,21,opt,name=node_report_sec,json=nodeReportSec,proto3" json:"node_report_sec,omitempty"`
	OnlineReportSec    uint32       `protobuf:"varint,22,opt,name=online_report_sec,json=onlineReportSec,proto3" json:"online_report_sec,omitempty"`
	NodeIds            []uint32     `protobuf:"varint,23,rep,packed,name=node_ids,json=nodeIds,proto3" json:"node_ids,omitempty"`
}

func (c *Config) Reset()         { *c = Config{} }
func (c *Config) String() string { return proto.CompactTextString(c) }
func (*Config) ProtoMessage()    {}

func (c *Config) GetNodeId() uint32 {
	if c != nil {
		return c.NodeId
	}
	return 0
}

func (c *Config) GetNodeIds() []uint32 {
	if c == nil {
		return nil
	}
	if len(c.NodeIds) > 0 {
		return c.NodeIds
	}
	if c.NodeId > 0 {
		return []uint32{c.NodeId}
	}
	return nil
}

func (c *Config) GetCheckRate() uint32 {
	if c != nil && c.CheckRate > 0 {
		return c.CheckRate
	}
	return 60
}

func (c *Config) GetMySQL() *MySQLConfig {
	if c != nil {
		return c.MySQL
	}
	return nil
}

func (c *Config) GetUseMySQL() uint32 {
	if c != nil {
		return c.UseMySQL
	}
	return 0
}

func (c *Config) GetTrafficReportInterval() uint32 {
	if c != nil && c.TrafficReportSec > 0 {
		return c.TrafficReportSec
	}
	return c.GetCheckRate()
}

func (c *Config) GetAliveIPReportInterval() uint32 {
	if c != nil && c.AliveIPReportSec > 0 {
		return c.AliveIPReportSec
	}
	return 60
}

func (c *Config) GetNodeReportInterval() uint32 {
	if c != nil && c.NodeReportSec > 0 {
		return c.NodeReportSec
	}
	return c.GetCheckRate()
}

func (c *Config) GetOnlineReportInterval() uint32 {
	if c != nil && c.OnlineReportSec > 0 {
		return c.OnlineReportSec
	}
	return c.GetCheckRate()
}

func init() {
	proto.RegisterType((*MySQLConfig)(nil), "v2ray.core.app.sspanel.MySQLConfig")
	proto.RegisterType((*Config)(nil), "v2ray.core.app.sspanel.Config")
}
