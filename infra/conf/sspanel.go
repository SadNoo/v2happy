package conf

import appsspanel "v2ray.com/core/app/sspanel"

type SSPanelMySQLConfig struct {
	Host     string `json:"host"`
	Port     uint32 `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	DBName   string `json:"dbname"`
}

type SSPanelConfig struct {
	NodeId             uint32              `json:"nodeid"`
	NodeIds            []uint32            `json:"nodeids"`
	CheckRate          uint32              `json:"checkRate"`
	SpeedTestCheckRate uint32              `json:"SpeedTestCheckRate"`
	PanelUrl           string              `json:"panelUrl"`
	PanelKey           string              `json:"panelKey"`
	DownWithPanel      uint32              `json:"downWithPanel"`
	MuRegex            string              `json:"mu_regex"`
	MuSuffix           string              `json:"mu_suffix"`
	MySQL              *SSPanelMySQLConfig `json:"mysql"`
	PanelType          string              `json:"paneltype"`
	UseMySQL           uint32              `json:"usemysql"`
	CFKey              string              `json:"cf_key"`
	CFEmail            string              `json:"cf_email"`
	ProxyTCP           bool                `json:"proxy_tcp"`
	AliKey             string              `json:"ali_key"`
	AliSecret          string              `json:"ali_secret"`
	CacheDurationSec   uint32              `json:"cache_duration_sec"`
	HTMLPath           string              `json:"html_path"`
	TrafficReportSec   uint32              `json:"trafficReportSec"`
	AliveIPReportSec   uint32              `json:"aliveIpReportSec"`
	NodeReportSec      uint32              `json:"nodeReportSec"`
	OnlineReportSec    uint32              `json:"onlineReportSec"`
}

func (c *SSPanelConfig) Build() (*appsspanel.Config, error) {
	config := &appsspanel.Config{
		NodeId:             c.NodeId,
		NodeIds:            c.NodeIds,
		CheckRate:          c.CheckRate,
		SpeedTestCheckRate: c.SpeedTestCheckRate,
		PanelUrl:           c.PanelUrl,
		PanelKey:           c.PanelKey,
		DownWithPanel:      c.DownWithPanel,
		MuRegex:            c.MuRegex,
		MuSuffix:           c.MuSuffix,
		PanelType:          c.PanelType,
		UseMySQL:           c.UseMySQL,
		CFKey:              c.CFKey,
		CFEmail:            c.CFEmail,
		ProxyTCP:           c.ProxyTCP,
		AliKey:             c.AliKey,
		AliSecret:          c.AliSecret,
		CacheDurationSec:   c.CacheDurationSec,
		HTMLPath:           c.HTMLPath,
		TrafficReportSec:   c.TrafficReportSec,
		AliveIPReportSec:   c.AliveIPReportSec,
		NodeReportSec:      c.NodeReportSec,
		OnlineReportSec:    c.OnlineReportSec,
	}
	if c.MySQL != nil {
		config.MySQL = &appsspanel.MySQLConfig{
			Host:     c.MySQL.Host,
			Port:     c.MySQL.Port,
			User:     c.MySQL.User,
			Password: c.MySQL.Password,
			DBName:   c.MySQL.DBName,
		}
	}
	return config, nil
}
