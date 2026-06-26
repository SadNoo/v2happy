// +build !confonly

package sspanel

//go:generate go run v2ray.com/core/common/errors/errorgen

import (
	"context"
	"database/sql"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"v2ray.com/core"
	"v2ray.com/core/app/proxyman"
	proxymanInbound "v2ray.com/core/app/proxyman/inbound"
	"v2ray.com/core/common"
	v2net "v2ray.com/core/common/net"
	"v2ray.com/core/common/protocol"
	"v2ray.com/core/common/serial"
	"v2ray.com/core/common/sspanelruntime"
	"v2ray.com/core/features/inbound"
	featureStats "v2ray.com/core/features/stats"
	"v2ray.com/core/proxy/vmess"
	vmessInbound "v2ray.com/core/proxy/vmess/inbound"
	"v2ray.com/core/transport/internet"
)

const mainInboundTag = "sspanel-vmess-main"

type App struct {
	ctx            context.Context
	config         *Config
	db             *sql.DB
	inboundManager inbound.Manager
	statsManager   featureStats.Manager

	ticker *time.Ticker
	done   chan struct{}

	access       sync.RWMutex
	users        map[int]panelUser
	aliveIPs     map[int]map[string]struct{}
	disconnected map[int]map[string]struct{}
	lastTraffic  map[int]trafficPair

	inboundSignature string
}

type panelNode struct {
	ID                 int
	Server             string
	NodeGroup          int
	NodeClass          int
	NodeSpeedLimit     int
	TrafficRate        float64
	NodeBandwidth      int64
	NodeBandwidthLimit int64
}

type panelUser struct {
	ID           int
	Email        string
	Passwd       string
	DisconnectIP string
}

type vmessNodeSettings struct {
	Host       string
	Port       int
	AlterID    uint32
	Network    string
	Header     string
	RawOptions []string
}

type trafficPair struct {
	Uplink   int64
	Downlink int64
}

func New(ctx context.Context, config *Config) (*App, error) {
	app := &App{
		ctx:          ctx,
		config:       config,
		done:         make(chan struct{}),
		users:        make(map[int]panelUser),
		aliveIPs:     make(map[int]map[string]struct{}),
		disconnected: make(map[int]map[string]struct{}),
		lastTraffic:  make(map[int]trafficPair),
	}

	if err := core.RequireFeatures(ctx, func(im inbound.Manager, sm featureStats.Manager) {
		app.inboundManager = im
		app.statsManager = sm
	}); err != nil {
		return nil, err
	}

	return app, nil
}

func (*App) Type() interface{} {
	return (*App)(nil)
}

func (a *App) Start() error {
	logInfo("SSPanel INFO: new config")
	logInfo("SSPanel INFO: using SSpanel direct MySQL mode")
	if a.config.GetUseMySQL() == 0 {
		return newError("SSPanel ERROR: only direct MySQL mode is implemented; set sspanel.usemysql to 1").AtError()
	}
	db, err := sql.Open("mysql", a.mysqlDSN())
	if err != nil {
		return err
	}
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(5 * time.Minute)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return err
	}
	a.db = db
	logInfo("SSPanel INFO: database connected")

	sspanelruntime.SetHook(a)

	if err := a.syncOnce(); err != nil {
		return err
	}

	a.ticker = time.NewTicker(time.Duration(a.config.GetCheckRate()) * time.Second)
	go a.loop()
	return nil
}

func (a *App) Close() error {
	if a.ticker != nil {
		a.ticker.Stop()
	}
	close(a.done)
	sspanelruntime.ClearHook(a)
	if a.inboundManager != nil {
		_ = a.inboundManager.RemoveHandler(context.Background(), mainInboundTag)
	}
	if a.db != nil {
		return a.db.Close()
	}
	return nil
}

func (a *App) RecordAliveIP(email string, ip string) {
	userID, ok := userIDFromEmail(email)
	if !ok {
		return
	}
	a.access.Lock()
	defer a.access.Unlock()
	if _, ok := a.aliveIPs[userID]; !ok {
		a.aliveIPs[userID] = make(map[string]struct{})
	}
	a.aliveIPs[userID][ip] = struct{}{}
}

func (a *App) ShouldReject(email string, ip string) bool {
	userID, ok := userIDFromEmail(email)
	if !ok {
		return false
	}
	a.access.RLock()
	defer a.access.RUnlock()
	blocked := a.disconnected[userID]
	if blocked == nil {
		return false
	}
	_, found := blocked[ip]
	return found
}

func (a *App) mysqlDSN() string {
	mysql := a.config.GetMySQL()
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=true&loc=Local",
		mysql.GetUser(), mysql.GetPassword(), mysql.GetHost(), mysql.GetPort(), mysql.GetDBName())
}

func (a *App) loop() {
	for {
		select {
		case <-a.ticker.C:
			if err := a.syncOnce(); err != nil {
				logWarning("SSPanel WARNING: sync failed", err)
			}
		case <-a.done:
			return
		}
	}
}

func (a *App) syncOnce() error {
	node, err := a.loadNode()
	if err != nil {
		return err
	}
	settings, err := parseVMessNodeServer(node.Server)
	if err != nil {
		return err
	}
	users, err := a.loadUsers(node)
	if err != nil {
		return err
	}
	alive := a.drainAliveIP()
	onlineUsers := len(alive)
	if err := a.collectTraffic(node, users, onlineUsers); err != nil {
		logWarning("SSPanel WARNING: failed to upload traffic", err)
	}
	if err := a.uploadAliveIP(alive); err != nil {
		logWarning("SSPanel WARNING: failed to upload alive ip", err)
	}
	if err := a.uploadNodeInfo(onlineUsers); err != nil {
		logWarning("SSPanel WARNING: failed to upload node info", err)
	}
	inboundUpdated, err := a.ensureInbound(settings, users)
	if err != nil {
		return err
	}
	a.storeUsers(users)
	logInfo("SSPanel INFO: sync completed, allowed_users=", len(users), " online_users=", onlineUsers, " inbound_updated=", inboundUpdated)
	return nil
}

func (a *App) loadNode() (panelNode, error) {
	var node panelNode
	var nodeSpeedLimit sql.NullString
	row := a.db.QueryRow(`
SELECT id, server, node_group, node_class, node_speedlimit, traffic_rate, node_bandwidth, node_bandwidth_limit
FROM ss_node
WHERE id = ?`, a.config.GetNodeId())
	err := row.Scan(&node.ID, &node.Server, &node.NodeGroup, &node.NodeClass, &nodeSpeedLimit, &node.TrafficRate, &node.NodeBandwidth, &node.NodeBandwidthLimit)
	if err != nil {
		return node, err
	}
	node.NodeSpeedLimit, err = parseDecimalInt(nodeSpeedLimit)
	if err != nil {
		return node, newError("SSPanel ERROR: invalid node_speedlimit").Base(err).AtError()
	}
	if node.NodeBandwidthLimit != 0 && node.NodeBandwidth >= node.NodeBandwidthLimit {
		return node, newError("SSPanel ERROR: node bandwidth limit reached").AtError()
	}
	return node, nil
}

func (a *App) loadUsers(node panelNode) ([]panelUser, error) {
	query := "SELECT id, email, passwd, COALESCE(disconnect_ip, '')\n" +
		"FROM `user`\n" +
		"WHERE ((class >= ? AND node_group = ?) OR is_admin = 1)\n" +
		"  AND enable = 1\n" +
		"  AND expire_in > NOW()\n" +
		"  AND transfer_enable > u + d"
	args := []interface{}{node.NodeClass, node.NodeGroup}
	if node.NodeGroup == 0 {
		query = "SELECT id, email, passwd, COALESCE(disconnect_ip, '')\n" +
			"FROM `user`\n" +
			"WHERE (class >= ? OR is_admin = 1)\n" +
			"  AND enable = 1\n" +
			"  AND expire_in > NOW()\n" +
			"  AND transfer_enable > u + d"
		args = []interface{}{node.NodeClass}
	}

	rows, err := a.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	users := make([]panelUser, 0, 256)
	for rows.Next() {
		var user panelUser
		if err := rows.Scan(&user.ID, &user.Email, &user.Passwd, &user.DisconnectIP); err != nil {
			return nil, err
		}
		users = append(users, user)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Slice(users, func(i, j int) bool { return users[i].ID < users[j].ID })
	return users, nil
}

func (a *App) ensureInbound(settings vmessNodeSettings, users []panelUser) (bool, error) {
	signature := inboundFingerprint(settings, users)
	if signature == a.inboundSignature {
		return false, nil
	}
	if err := a.replaceInbound(settings, users); err != nil {
		return false, err
	}
	a.inboundSignature = signature
	return true, nil
}

func (a *App) replaceInbound(settings vmessNodeSettings, users []panelUser) error {
	port, err := v2net.PortFromInt(uint32(settings.Port))
	if err != nil {
		return err
	}

	vmessUsers := make([]*protocol.User, 0, len(users))
	for _, user := range users {
		account := &vmess.Account{
			Id:      userUUID(user.ID, user.Passwd),
			AlterId: settings.AlterID,
			SecuritySettings: &protocol.SecurityConfig{
				Type: protocol.SecurityType_AUTO,
			},
		}
		vmessUsers = append(vmessUsers, &protocol.User{
			Level:   0,
			Email:   userEmail(user.ID),
			Account: serial.ToTypedMessage(account),
		})
	}

	stream := &internet.StreamConfig{Protocol: internet.TransportProtocol_TCP}
	receiver := &proxyman.ReceiverConfig{
		Listen:         v2net.NewIPOrDomain(v2net.AnyIP),
		PortRange:      v2net.SinglePortRange(port),
		StreamSettings: stream,
	}
	inboundConfig := &core.InboundHandlerConfig{
		Tag:              mainInboundTag,
		ReceiverSettings: serial.ToTypedMessage(receiver),
		ProxySettings: serial.ToTypedMessage(&vmessInbound.Config{
			User: vmessUsers,
			Default: &vmessInbound.DefaultConfig{
				AlterId: settings.AlterID,
				Level:   0,
			},
		}),
	}
	handler, err := proxymanInbound.NewHandler(a.ctx, inboundConfig)
	if err != nil {
		return err
	}
	_ = a.inboundManager.RemoveHandler(context.Background(), mainInboundTag)
	if err := a.inboundManager.AddHandler(context.Background(), handler); err != nil {
		return err
	}
	logInfo("SSPanel INFO: inbound rebuilt, listen=0.0.0.0:", settings.Port, " users=", len(users))
	return nil
}

func (a *App) collectTraffic(node panelNode, users []panelUser, onlineUsers int) error {
	if a.statsManager == nil {
		return nil
	}
	now := time.Now().Unix()
	totalU := int64(0)
	totalD := int64(0)
	updated := make(map[int]trafficPair)

	tx, err := a.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, user := range users {
		email := userEmail(user.ID)
		current := trafficPair{
			Uplink:   counterValue(a.statsManager, "user>>>"+email+">>>traffic>>>uplink"),
			Downlink: counterValue(a.statsManager, "user>>>"+email+">>>traffic>>>downlink"),
		}
		last := a.lastTraffic[user.ID]
		deltaU := current.Uplink - last.Uplink
		deltaD := current.Downlink - last.Downlink
		if deltaU < 0 || deltaD < 0 {
			deltaU = 0
			deltaD = 0
		}
		updated[user.ID] = current
		if deltaU == 0 && deltaD == 0 {
			continue
		}
		ratedU := int64(math.Round(float64(deltaU) * node.TrafficRate))
		ratedD := int64(math.Round(float64(deltaD) * node.TrafficRate))
		if _, err := tx.Exec("UPDATE `user` SET t = ?, u = u + ?, d = d + ? WHERE id = ?", now, ratedU, ratedD, user.ID); err != nil {
			return err
		}
		if _, err := tx.Exec(
			"INSERT INTO user_traffic_log (user_id, u, d, node_id, rate, traffic, log_time) VALUES (?, ?, ?, ?, ?, ?, ?)",
			user.ID, deltaU, deltaD, node.ID, node.TrafficRate, formatTraffic(float64(deltaU+deltaD)*node.TrafficRate), now,
		); err != nil {
			return err
		}
		totalU += deltaU
		totalD += deltaD
	}

	if totalU+totalD > 0 {
		if _, err := tx.Exec("UPDATE ss_node SET node_bandwidth = node_bandwidth + ?, node_heartbeat = ? WHERE id = ?", totalU+totalD, now, node.ID); err != nil {
			return err
		}
	}
	if _, err := tx.Exec("INSERT INTO ss_node_online_log (node_id, online_user, log_time) VALUES (?, ?, ?)", node.ID, onlineUsers, now); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}

	for userID, traffic := range updated {
		a.lastTraffic[userID] = traffic
	}
	return nil
}

func (a *App) drainAliveIP() map[int]map[string]struct{} {
	a.access.Lock()
	alive := a.aliveIPs
	a.aliveIPs = make(map[int]map[string]struct{})
	a.access.Unlock()
	return alive
}

func (a *App) uploadAliveIP(alive map[int]map[string]struct{}) error {
	if len(alive) == 0 {
		return nil
	}
	now := time.Now().Unix()
	tx, err := a.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for userID, ips := range alive {
		for ip := range ips {
			if _, err := tx.Exec("INSERT INTO alive_ip (nodeid, userid, ip, datetime) VALUES (?, ?, ?, ?)", a.config.GetNodeId(), userID, ip, now); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

func (a *App) uploadNodeInfo(onlineUsers int) error {
	load := readLoadAverage()
	uptime := readUptime()
	now := time.Now().Unix()
	_, err := a.db.Exec(
		"INSERT INTO ss_node_info (node_id, uptime, `load`, log_time) VALUES (?, ?, ?, ?)",
		a.config.GetNodeId(), uptime, load, now,
	)
	return err
}

func (a *App) storeUsers(users []panelUser) {
	userMap := make(map[int]panelUser, len(users))
	disconnected := make(map[int]map[string]struct{})
	for _, user := range users {
		userMap[user.ID] = user
		blocked := parseIPList(user.DisconnectIP)
		if len(blocked) > 0 {
			disconnected[user.ID] = blocked
		}
	}
	a.access.Lock()
	a.users = userMap
	a.disconnected = disconnected
	a.access.Unlock()

	for userID := range a.lastTraffic {
		if _, ok := userMap[userID]; !ok {
			delete(a.lastTraffic, userID)
		}
	}
}

func parseVMessNodeServer(server string) (vmessNodeSettings, error) {
	parts := strings.Split(server, ";")
	for len(parts) < 6 {
		parts = append(parts, "")
	}
	port := 443
	if strings.TrimSpace(parts[1]) != "" && strings.TrimSpace(parts[1]) != "0" {
		parsed, err := strconv.Atoi(strings.TrimSpace(parts[1]))
		if err != nil {
			return vmessNodeSettings{}, err
		}
		port = parsed
	}
	alterID := uint32(0)
	if strings.TrimSpace(parts[2]) != "" {
		parsed, err := strconv.ParseUint(strings.TrimSpace(parts[2]), 10, 32)
		if err != nil {
			return vmessNodeSettings{}, err
		}
		alterID = uint32(parsed)
	}
	settings := vmessNodeSettings{
		Host:       strings.TrimSpace(parts[0]),
		Port:       port,
		AlterID:    alterID,
		Network:    strings.ToLower(strings.TrimSpace(parts[3])),
		Header:     strings.TrimSpace(parts[4]),
		RawOptions: strings.Split(parts[5], "|"),
	}
	if settings.Network == "" {
		settings.Network = "tcp"
	}
	for _, option := range settings.RawOptions {
		pair := strings.SplitN(option, "=", 2)
		if len(pair) == 2 && strings.TrimSpace(pair[0]) == "outside_port" {
			parsed, err := strconv.Atoi(strings.TrimSpace(pair[1]))
			if err != nil {
				return vmessNodeSettings{}, err
			}
			settings.Port = parsed
		}
	}
	if settings.Network != "tcp" {
		return settings, newError("SSPanel ERROR: only VMess TCP inbound is implemented in this build, got network=", settings.Network).AtError()
	}
	return settings, nil
}

func inboundFingerprint(settings vmessNodeSettings, users []panelUser) string {
	hash := fnv.New64a()
	_, _ = fmt.Fprintf(hash, "port=%d;alterID=%d;network=%s;", settings.Port, settings.AlterID, settings.Network)
	for _, user := range users {
		_, _ = fmt.Fprintf(hash, "%d:%s;", user.ID, user.Passwd)
	}
	return strconv.FormatUint(hash.Sum64(), 16)
}

func userEmail(userID int) string {
	return "sspanel-user-" + strconv.Itoa(userID)
}

func userIDFromEmail(email string) (int, bool) {
	if !strings.HasPrefix(email, "sspanel-user-") {
		return 0, false
	}
	id, err := strconv.Atoi(strings.TrimPrefix(email, "sspanel-user-"))
	return id, err == nil
}

func counterValue(manager featureStats.Manager, name string) int64 {
	counter := manager.GetCounter(name)
	if counter == nil {
		return 0
	}
	return counter.Value()
}

func parseIPList(raw string) map[string]struct{} {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	var result map[string]struct{}
	for _, item := range strings.FieldsFunc(raw, func(r rune) bool {
		return r == ',' || r == '\n' || r == '\r' || r == '\t' || r == ' '
	}) {
		item = strings.TrimSpace(item)
		if item != "" {
			if result == nil {
				result = make(map[string]struct{})
			}
			result[item] = struct{}{}
		}
	}
	return result
}

func parseDecimalInt(value sql.NullString) (int, error) {
	if !value.Valid {
		return 0, nil
	}
	raw := strings.TrimSpace(value.String)
	if raw == "" {
		return 0, nil
	}
	parsed, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, err
	}
	return int(math.Round(parsed)), nil
}

func formatTraffic(bytes float64) string {
	units := []string{"B", "KB", "MB", "GB", "TB", "PB"}
	value := bytes
	unit := units[0]
	for _, u := range units {
		unit = u
		if value < 1024 || u == units[len(units)-1] {
			break
		}
		value /= 1024
	}
	return fmt.Sprintf("%.2f %s", value, unit)
}

func readLoadAverage() string {
	data, err := ioutil.ReadFile("/proc/loadavg")
	if err != nil {
		return "0 0 0"
	}
	fields := strings.Fields(string(data))
	if len(fields) >= 3 {
		return strings.Join(fields[:3], " ")
	}
	return strings.TrimSpace(string(data))
}

func readUptime() float64 {
	data, err := ioutil.ReadFile("/proc/uptime")
	if err != nil {
		return 0
	}
	fields := strings.Fields(string(data))
	if len(fields) == 0 {
		return 0
	}
	uptime, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return 0
	}
	return uptime
}

func logInfo(values ...interface{}) {
	newError(values...).AtInfo().WriteToLog()
}

func logWarning(message string, err error) {
	newError(message).Base(err).AtWarning().WriteToLog()
}

func init() {
	common.Must(common.RegisterConfig((*Config)(nil), func(ctx context.Context, config interface{}) (interface{}, error) {
		return New(ctx, config.(*Config))
	}))
}
