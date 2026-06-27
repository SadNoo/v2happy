// +build !confonly

package sspanel

//go:generate go run v2ray.com/core/common/errors/errorgen

import (
	"context"
	"database/sql"
	"errors"
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

const mainInboundTagPrefix = "sspanel-vmess-node-"
const reportBatchSize = 200
const reportFailureRetrySeconds = 30
const dbOperationTimeout = 15 * time.Second

var (
	errNodeNotFound              = errors.New("sspanel node not found")
	errNodeBandwidthLimitReached = errors.New("sspanel node bandwidth limit reached")
)

type App struct {
	ctx            context.Context
	config         *Config
	db             *sql.DB
	inboundManager inbound.Manager
	statsManager   featureStats.Manager

	ticker *time.Ticker
	done   chan struct{}

	nodes      map[int]*nodeRuntime
	nodeOrder  []int
	multiNode  bool
	portAccess sync.Mutex
	portOwners map[int]int
}

type nodeRuntime struct {
	app        *App
	nodeID     int
	inboundTag string
	multiNode  bool

	access        sync.RWMutex
	users         map[int]panelUser
	retiringUsers map[int]panelUser
	aliveIPs      map[int]map[string]struct{}
	disconnected  map[int]map[string]struct{}
	lastTraffic   map[int]trafficPair

	inboundSignature  string
	inboundSettings   vmessNodeSettings
	inboundUsers      []panelUser
	cachedUsers       []panelUser
	userListSignature string
	lastTrafficReport time.Time
	lastAliveIPReport time.Time
	lastNodeReport    time.Time
	lastOnlineReport  time.Time
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
	ID                  int
	Passwd              string
	DisconnectIP        string
	VmessEmail          string
	VmessUUID           string
	UplinkCounterName   string
	DownlinkCounterName string
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

type trafficDelta struct {
	UserID int
	U      int64
	D      int64
	RatedU int64
	RatedD int64
	Rate   float64
}

type trafficReportStats struct {
	Users              int
	RawU               int64
	RawD               int64
	NodeBandwidthDelta int64
}

type aliveIPRecord struct {
	UserID int
	IP     string
}

func New(ctx context.Context, config *Config) (*App, error) {
	if config == nil {
		return nil, newError("SSPanel ERROR: config is required").AtError()
	}
	nodeIDs, err := normalizeNodeIDs(config.GetNodeIds())
	if err != nil {
		return nil, err
	}
	app := &App{
		ctx:        ctx,
		config:     config,
		done:       make(chan struct{}),
		nodes:      make(map[int]*nodeRuntime, len(nodeIDs)),
		nodeOrder:  make([]int, 0, len(nodeIDs)),
		multiNode:  len(nodeIDs) > 1,
		portOwners: make(map[int]int),
	}
	for _, nodeID := range nodeIDs {
		rt := newNodeRuntime(app, int(nodeID), len(nodeIDs) > 1)
		app.nodes[rt.nodeID] = rt
		app.nodeOrder = append(app.nodeOrder, rt.nodeID)
	}

	if err := core.RequireFeatures(ctx, func(im inbound.Manager, sm featureStats.Manager) {
		app.inboundManager = im
		app.statsManager = sm
	}); err != nil {
		return nil, err
	}

	return app, nil
}

func normalizeNodeIDs(raw []uint32) ([]uint32, error) {
	if len(raw) == 0 {
		return nil, newError("SSPanel ERROR: node_id is required").AtError()
	}
	seen := make(map[uint32]struct{}, len(raw))
	result := make([]uint32, 0, len(raw))
	for _, nodeID := range raw {
		if nodeID == 0 {
			return nil, newError("SSPanel ERROR: node_id must be greater than 0").AtError()
		}
		if _, ok := seen[nodeID]; ok {
			return nil, newError("SSPanel ERROR: duplicate node_id=", nodeID).AtError()
		}
		seen[nodeID] = struct{}{}
		result = append(result, nodeID)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result, nil
}

func newNodeRuntime(app *App, nodeID int, multiNode bool) *nodeRuntime {
	return &nodeRuntime{
		app:           app,
		nodeID:        nodeID,
		inboundTag:    inboundTag(nodeID),
		multiNode:     multiNode,
		users:         make(map[int]panelUser),
		retiringUsers: make(map[int]panelUser),
		aliveIPs:      make(map[int]map[string]struct{}),
		disconnected:  make(map[int]map[string]struct{}),
		lastTraffic:   make(map[int]trafficPair),
	}
}

func (*App) Type() interface{} {
	return (*App)(nil)
}

func (a *App) Start() error {
	logInfo("SSPanel INFO: new config")
	logInfo("SSPanel INFO: using SSpanel direct MySQL mode")
	logInfo("SSPanel INFO: configured nodes=", a.nodeOrder)
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
		for _, rt := range a.nodes {
			_ = a.inboundManager.RemoveHandler(context.Background(), rt.inboundTag)
		}
	}
	if a.db != nil {
		return a.db.Close()
	}
	return nil
}

func (a *App) RecordAliveIP(email string, ip string) {
	rt, userID, ok := a.nodeByUserEmail(email)
	if !ok {
		return
	}
	rt.recordAliveIP(userID, ip)
}

func (rt *nodeRuntime) recordAliveIP(userID int, ip string) {
	rt.access.RLock()
	if ips := rt.aliveIPs[userID]; ips != nil {
		if _, found := ips[ip]; found {
			rt.access.RUnlock()
			return
		}
	}
	rt.access.RUnlock()

	rt.access.Lock()
	defer rt.access.Unlock()
	if rt.aliveIPs == nil {
		rt.aliveIPs = make(map[int]map[string]struct{})
	}
	if _, ok := rt.aliveIPs[userID]; !ok {
		rt.aliveIPs[userID] = make(map[string]struct{})
	}
	rt.aliveIPs[userID][ip] = struct{}{}
}

func (a *App) ShouldReject(email string, ip string) bool {
	rt, userID, ok := a.nodeByUserEmail(email)
	if !ok {
		return false
	}
	return rt.shouldReject(userID, ip)
}

func (rt *nodeRuntime) shouldReject(userID int, ip string) bool {
	rt.access.RLock()
	defer rt.access.RUnlock()
	blocked := rt.disconnected[userID]
	if blocked == nil {
		return false
	}
	_, found := blocked[ip]
	return found
}

func (a *App) nodeByUserEmail(email string) (*nodeRuntime, int, bool) {
	nodeID, userID, hasNode, ok := userRefFromEmail(email)
	if !ok {
		return nil, 0, false
	}
	if hasNode {
		rt := a.nodes[nodeID]
		return rt, userID, rt != nil
	}
	if len(a.nodeOrder) != 1 {
		return nil, 0, false
	}
	rt := a.nodes[a.nodeOrder[0]]
	return rt, userID, rt != nil
}

func (a *App) mysqlDSN() string {
	mysql := a.config.GetMySQL()
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=true&loc=Local",
		mysql.GetUser(), mysql.GetPassword(), mysql.GetHost(), mysql.GetPort(), mysql.GetDBName())
}

func (a *App) claimPort(nodeID int, port int) error {
	a.portAccess.Lock()
	defer a.portAccess.Unlock()
	if owner, ok := a.portOwners[port]; ok && owner != nodeID {
		return newError("SSPanel ERROR: duplicate node port=", port, " owner_node_id=", owner, " conflict_node_id=", nodeID).AtError()
	}
	for ownedPort, owner := range a.portOwners {
		if owner == nodeID && ownedPort != port {
			delete(a.portOwners, ownedPort)
		}
	}
	a.portOwners[port] = nodeID
	return nil
}

func (a *App) releasePort(nodeID int) {
	a.portAccess.Lock()
	for port, owner := range a.portOwners {
		if owner == nodeID {
			delete(a.portOwners, port)
		}
	}
	a.portAccess.Unlock()
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
	var firstErr error
	successCount := 0
	for _, nodeID := range a.nodeOrder {
		rt := a.nodes[nodeID]
		if rt == nil {
			continue
		}
		if err := rt.syncOnce(); err != nil {
			logWarning("SSPanel WARNING: sync failed, node_id="+strconv.Itoa(nodeID), err)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if rt.inboundSignature != "" {
			successCount++
		}
	}
	if a.multiNode && successCount > 0 {
		return nil
	}
	if successCount == 0 {
		if firstErr != nil {
			return firstErr
		}
		return newError("SSPanel ERROR: no active inbound after sync").AtError()
	}
	return firstErr
}

func (rt *nodeRuntime) syncOnce() error {
	a := rt.app
	now := time.Now()
	node, err := a.loadNode(rt.nodeID)
	if err != nil {
		if errors.Is(err, errNodeNotFound) || errors.Is(err, errNodeBandwidthLimitReached) {
			if removeErr := rt.disableInbound(err.Error()); removeErr != nil {
				return removeErr
			}
			logWarning("SSPanel WARNING: node unavailable; inbound disabled, node_id="+strconv.Itoa(rt.nodeID), err)
			return nil
		}
		return err
	}
	if err := a.uploadNodeHeartbeat(node.ID, now); err != nil {
		return err
	}
	settings, err := parseVMessNodeServer(node.Server)
	if err != nil {
		if removeErr := rt.disableInbound(err.Error()); removeErr != nil {
			return removeErr
		}
		logWarning("SSPanel WARNING: invalid node server config; inbound disabled, node_id="+strconv.Itoa(rt.nodeID), err)
		return nil
	}
	if err := a.claimPort(rt.nodeID, settings.Port); err != nil {
		if removeErr := rt.disableInbound(err.Error()); removeErr != nil {
			return removeErr
		}
		logWarning("SSPanel WARNING: inbound port conflict; node disabled, node_id="+strconv.Itoa(rt.nodeID), err)
		return nil
	}
	users, usersReloaded, err := rt.loadUsersCached(node)
	if err != nil {
		return err
	}
	inboundUpdated, err := rt.ensureInbound(settings, users, usersReloaded)
	if err != nil {
		a.releasePort(rt.nodeID)
		if rt.inboundSignature != "" && rt.inboundSettings.Port > 0 {
			_ = a.claimPort(rt.nodeID, rt.inboundSettings.Port)
		}
		return err
	}
	if usersReloaded {
		rt.storeUsers(users)
		rt.compactCachedUsers()
	}
	trafficUsers := rt.trafficUsers(users)

	onlineUsers := rt.aliveUserCount()
	trafficReported := false
	trafficStats := trafficReportStats{}
	aliveReported := false
	aliveRecords := 0
	nodeReported := false
	onlineReported := false

	if reportDue(rt.lastTrafficReport, a.config.GetTrafficReportInterval(), now) {
		trafficReported = true
		activeTraffic, stats, err := rt.collectTraffic(node, trafficUsers)
		if err != nil {
			markReportFailure(&rt.lastTrafficReport, a.config.GetTrafficReportInterval(), now)
			logWarning("SSPanel WARNING: failed to upload traffic, node_id="+strconv.Itoa(rt.nodeID), err)
		} else {
			markReportSuccess(&rt.lastTrafficReport, now)
			trafficStats = stats
			rt.pruneRetiringUsers(activeTraffic)
		}
	}
	if reportDue(rt.lastOnlineReport, a.config.GetOnlineReportInterval(), now) {
		onlineReported = true
		if err := a.uploadOnlineLog(node.ID, onlineUsers); err != nil {
			markReportFailure(&rt.lastOnlineReport, a.config.GetOnlineReportInterval(), now)
			logWarning("SSPanel WARNING: failed to upload online log, node_id="+strconv.Itoa(rt.nodeID), err)
		} else {
			markReportSuccess(&rt.lastOnlineReport, now)
		}
	}
	if reportDue(rt.lastNodeReport, a.config.GetNodeReportInterval(), now) {
		nodeReported = true
		if err := a.uploadNodeInfo(node.ID, onlineUsers); err != nil {
			markReportFailure(&rt.lastNodeReport, a.config.GetNodeReportInterval(), now)
			logWarning("SSPanel WARNING: failed to upload node info, node_id="+strconv.Itoa(rt.nodeID), err)
		} else {
			markReportSuccess(&rt.lastNodeReport, now)
		}
	}
	if reportDue(rt.lastAliveIPReport, a.config.GetAliveIPReportInterval(), now) {
		aliveReported = true
		alive := rt.drainAliveIP()
		records, err := rt.uploadAliveIP(alive)
		if err != nil {
			markReportFailure(&rt.lastAliveIPReport, a.config.GetAliveIPReportInterval(), now)
			rt.restoreAliveIP(alive)
			logWarning("SSPanel WARNING: failed to upload alive ip, node_id="+strconv.Itoa(rt.nodeID), err)
		} else {
			markReportSuccess(&rt.lastAliveIPReport, now)
			aliveRecords = records
		}
	}

	logInfo("SSPanel INFO: sync completed, node_id=", rt.nodeID, " allowed_users=", len(users), " users_reloaded=", usersReloaded, " online_users=", onlineUsers, " inbound_updated=", inboundUpdated, " traffic_reported=", trafficReported, " traffic_users=", trafficStats.Users, " raw_upload=", trafficStats.RawU, " raw_download=", trafficStats.RawD, " node_bandwidth_delta=", trafficStats.NodeBandwidthDelta, " alive_reported=", aliveReported, " alive_records=", aliveRecords, " node_reported=", nodeReported, " online_reported=", onlineReported)
	return nil
}

func (a *App) loadNode(nodeID int) (panelNode, error) {
	var node panelNode
	var nodeSpeedLimit sql.NullString
	ctx, cancel := context.WithTimeout(a.ctx, dbOperationTimeout)
	defer cancel()
	row := a.db.QueryRowContext(ctx, `
SELECT id, server, node_group, node_class, node_speedlimit, traffic_rate, node_bandwidth, node_bandwidth_limit
FROM ss_node
WHERE id = ?`, nodeID)
	err := row.Scan(&node.ID, &node.Server, &node.NodeGroup, &node.NodeClass, &nodeSpeedLimit, &node.TrafficRate, &node.NodeBandwidth, &node.NodeBandwidthLimit)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return node, errNodeNotFound
		}
		return node, err
	}
	node.NodeSpeedLimit, err = parseDecimalInt(nodeSpeedLimit)
	if err != nil {
		return node, newError("SSPanel ERROR: invalid node_speedlimit").Base(err).AtError()
	}
	if node.NodeBandwidthLimit != 0 && node.NodeBandwidth >= node.NodeBandwidthLimit {
		return node, fmt.Errorf("%w: bandwidth=%d limit=%d", errNodeBandwidthLimitReached, node.NodeBandwidth, node.NodeBandwidthLimit)
	}
	return node, nil
}

func (a *App) loadUsers(node panelNode) ([]panelUser, error) {
	query := "SELECT id, passwd, COALESCE(disconnect_ip, '')\n" +
		"FROM `user`\n" +
		"WHERE ((class >= ? AND node_group = ?) OR is_admin = 1)\n" +
		"  AND enable = 1\n" +
		"  AND expire_in > NOW()\n" +
		"  AND transfer_enable > u + d"
	args := []interface{}{node.NodeClass, node.NodeGroup}
	if node.NodeGroup == 0 {
		query = "SELECT id, passwd, COALESCE(disconnect_ip, '')\n" +
			"FROM `user`\n" +
			"WHERE (class >= ? OR is_admin = 1)\n" +
			"  AND enable = 1\n" +
			"  AND expire_in > NOW()\n" +
			"  AND transfer_enable > u + d"
		args = []interface{}{node.NodeClass}
	}

	ctx, cancel := context.WithTimeout(a.ctx, dbOperationTimeout)
	defer cancel()
	rows, err := a.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	users := make([]panelUser, 0, 256)
	for rows.Next() {
		var user panelUser
		if err := rows.Scan(&user.ID, &user.Passwd, &user.DisconnectIP); err != nil {
			return nil, err
		}
		user.prepareRuntimeFields(node.ID, a.multiNode)
		users = append(users, user)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Slice(users, func(i, j int) bool { return users[i].ID < users[j].ID })
	return users, nil
}

func (rt *nodeRuntime) loadUsersCached(node panelNode) ([]panelUser, bool, error) {
	signature, err := rt.app.loadUserListSignature(node)
	if err != nil {
		return nil, false, err
	}
	if rt.cachedUsers != nil && signature == rt.userListSignature {
		return rt.cachedUsers, false, nil
	}
	users, err := rt.app.loadUsers(node)
	if err != nil {
		return nil, false, err
	}
	rt.cachedUsers = users
	rt.userListSignature = signature
	return users, true, nil
}

func (a *App) loadUserListSignature(node panelNode) (string, error) {
	query := "SELECT CAST(COUNT(*) AS CHAR), CAST(COALESCE(SUM(id), 0) AS CHAR), CAST(COALESCE(SUM(CRC32(CONCAT_WS('#', id, passwd, COALESCE(disconnect_ip, '')))), 0) AS CHAR)\n" +
		"FROM `user`\n" +
		"WHERE ((class >= ? AND node_group = ?) OR is_admin = 1)\n" +
		"  AND enable = 1\n" +
		"  AND expire_in > NOW()\n" +
		"  AND transfer_enable > u + d"
	args := []interface{}{node.NodeClass, node.NodeGroup}
	if node.NodeGroup == 0 {
		query = "SELECT CAST(COUNT(*) AS CHAR), CAST(COALESCE(SUM(id), 0) AS CHAR), CAST(COALESCE(SUM(CRC32(CONCAT_WS('#', id, passwd, COALESCE(disconnect_ip, '')))), 0) AS CHAR)\n" +
			"FROM `user`\n" +
			"WHERE (class >= ? OR is_admin = 1)\n" +
			"  AND enable = 1\n" +
			"  AND expire_in > NOW()\n" +
			"  AND transfer_enable > u + d"
		args = []interface{}{node.NodeClass}
	}
	var count, idSum, crcSum string
	ctx, cancel := context.WithTimeout(a.ctx, dbOperationTimeout)
	defer cancel()
	if err := a.db.QueryRowContext(ctx, query, args...).Scan(&count, &idSum, &crcSum); err != nil {
		return "", err
	}
	return strconv.Itoa(node.NodeClass) + ":" + strconv.Itoa(node.NodeGroup) + ":" + count + ":" + idSum + ":" + crcSum, nil
}

func (rt *nodeRuntime) ensureInbound(settings vmessNodeSettings, users []panelUser, usersReloaded bool) (bool, error) {
	if !usersReloaded && rt.inboundSignature != "" && sameInboundSettings(settings, rt.inboundSettings) {
		return false, nil
	}
	signature := inboundFingerprint(settings, users)
	if signature == rt.inboundSignature {
		return false, nil
	}
	if err := rt.replaceInbound(settings, users); err != nil {
		return false, err
	}
	rt.inboundSignature = signature
	return true, nil
}

func (rt *nodeRuntime) replaceInbound(settings vmessNodeSettings, users []panelUser) error {
	handler, err := rt.newInboundHandler(settings, users)
	if err != nil {
		return err
	}
	oldSettings := rt.inboundSettings
	oldUsers := rt.inboundUsers
	hadOld := rt.inboundSignature != ""

	_ = rt.app.inboundManager.RemoveHandler(context.Background(), rt.inboundTag)
	if err := rt.app.inboundManager.AddHandler(context.Background(), handler); err != nil {
		if hadOld {
			if restoreErr := rt.restoreInbound(oldSettings, oldUsers); restoreErr != nil {
				logWarning("SSPanel WARNING: failed to restore previous inbound after replace failure, node_id="+strconv.Itoa(rt.nodeID), restoreErr)
			}
		}
		return err
	}
	rt.inboundSettings = settings
	rt.inboundUsers = users
	logInfo("SSPanel INFO: inbound rebuilt, node_id=", rt.nodeID, " listen=0.0.0.0:", settings.Port, " users=", len(users))
	return nil
}

func (rt *nodeRuntime) newInboundHandler(settings vmessNodeSettings, users []panelUser) (inbound.Handler, error) {
	port, err := v2net.PortFromInt(uint32(settings.Port))
	if err != nil {
		return nil, err
	}

	vmessUsers := make([]*protocol.User, 0, len(users))
	for _, user := range users {
		account := &vmess.Account{
			Id:      user.VmessUUID,
			AlterId: settings.AlterID,
			SecuritySettings: &protocol.SecurityConfig{
				Type: protocol.SecurityType_AUTO,
			},
		}
		vmessUsers = append(vmessUsers, &protocol.User{
			Level:   0,
			Email:   user.VmessEmail,
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
		Tag:              rt.inboundTag,
		ReceiverSettings: serial.ToTypedMessage(receiver),
		ProxySettings: serial.ToTypedMessage(&vmessInbound.Config{
			User: vmessUsers,
			Default: &vmessInbound.DefaultConfig{
				AlterId: settings.AlterID,
				Level:   0,
			},
		}),
	}
	return proxymanInbound.NewHandler(rt.app.ctx, inboundConfig)
}

func (rt *nodeRuntime) restoreInbound(settings vmessNodeSettings, users []panelUser) error {
	handler, err := rt.newInboundHandler(settings, users)
	if err != nil {
		return err
	}
	if err := rt.app.inboundManager.AddHandler(context.Background(), handler); err != nil {
		return err
	}
	logInfo("SSPanel INFO: previous inbound restored, node_id=", rt.nodeID, " listen=0.0.0.0:", settings.Port, " users=", len(users))
	return nil
}

func (rt *nodeRuntime) disableInbound(reason string) error {
	if rt.app.inboundManager != nil {
		_ = rt.app.inboundManager.RemoveHandler(context.Background(), rt.inboundTag)
	}
	rt.app.releasePort(rt.nodeID)
	rt.inboundSignature = ""
	rt.inboundSettings = vmessNodeSettings{}
	rt.inboundUsers = nil
	rt.cachedUsers = nil
	rt.userListSignature = ""
	rt.access.Lock()
	rt.users = make(map[int]panelUser)
	rt.retiringUsers = make(map[int]panelUser)
	rt.disconnected = make(map[int]map[string]struct{})
	rt.access.Unlock()
	logInfo("SSPanel INFO: inbound disabled, node_id=", rt.nodeID, " reason=", reason)
	return nil
}

func (rt *nodeRuntime) collectTraffic(node panelNode, users []panelUser) (map[int]struct{}, trafficReportStats, error) {
	a := rt.app
	if a.statsManager == nil {
		return nil, trafficReportStats{}, nil
	}
	now := time.Now().Unix()
	totalU := int64(0)
	totalD := int64(0)
	updated := make(map[int]trafficPair, len(users))
	deltas := make([]trafficDelta, 0, 128)
	var activeTraffic map[int]struct{}
	trackRetiringTraffic := len(rt.retiringUsers) > 0

	for _, user := range users {
		current := trafficPair{
			Uplink:   counterValue(a.statsManager, user.UplinkCounterName),
			Downlink: counterValue(a.statsManager, user.DownlinkCounterName),
		}
		last := rt.lastTraffic[user.ID]
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
		if trackRetiringTraffic {
			if activeTraffic == nil {
				activeTraffic = make(map[int]struct{})
			}
			activeTraffic[user.ID] = struct{}{}
		}
		ratedU := int64(math.Round(float64(deltaU) * node.TrafficRate))
		ratedD := int64(math.Round(float64(deltaD) * node.TrafficRate))
		deltas = append(deltas, trafficDelta{
			UserID: user.ID,
			U:      deltaU,
			D:      deltaD,
			RatedU: ratedU,
			RatedD: ratedD,
			Rate:   node.TrafficRate,
		})
		totalU += deltaU
		totalD += deltaD
	}

	if len(deltas) == 0 {
		for userID, traffic := range updated {
			rt.lastTraffic[userID] = traffic
		}
		return activeTraffic, trafficReportStats{}, nil
	}

	ctx, cancel := context.WithTimeout(a.ctx, dbOperationTimeout)
	defer cancel()
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, trafficReportStats{}, err
	}
	defer tx.Rollback()

	for start := 0; start < len(deltas); start += reportBatchSize {
		end := start + reportBatchSize
		if end > len(deltas) {
			end = len(deltas)
		}
		chunk := deltas[start:end]
		if err := updateUserTrafficBatch(ctx, tx, now, chunk); err != nil {
			return nil, trafficReportStats{}, err
		}
		if err := insertTrafficLogBatch(ctx, tx, node.ID, now, chunk); err != nil {
			return nil, trafficReportStats{}, err
		}
	}

	if totalU+totalD > 0 {
		if _, err := tx.ExecContext(ctx, "UPDATE ss_node SET node_bandwidth = node_bandwidth + ?, node_heartbeat = ? WHERE id = ?", totalU+totalD, now, node.ID); err != nil {
			return nil, trafficReportStats{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, trafficReportStats{}, err
	}

	for userID, traffic := range updated {
		rt.lastTraffic[userID] = traffic
	}
	return activeTraffic, trafficReportStats{
		Users:              len(deltas),
		RawU:               totalU,
		RawD:               totalD,
		NodeBandwidthDelta: totalU + totalD,
	}, nil
}

func (rt *nodeRuntime) drainAliveIP() map[int]map[string]struct{} {
	rt.access.Lock()
	if len(rt.aliveIPs) == 0 {
		rt.access.Unlock()
		return nil
	}
	alive := rt.aliveIPs
	rt.aliveIPs = make(map[int]map[string]struct{}, len(alive))
	rt.access.Unlock()
	return alive
}

func (rt *nodeRuntime) restoreAliveIP(alive map[int]map[string]struct{}) {
	if len(alive) == 0 {
		return
	}
	rt.access.Lock()
	if rt.aliveIPs == nil {
		rt.aliveIPs = make(map[int]map[string]struct{}, len(alive))
	}
	for userID, ips := range alive {
		if _, ok := rt.aliveIPs[userID]; !ok {
			rt.aliveIPs[userID] = make(map[string]struct{}, len(ips))
		}
		for ip := range ips {
			rt.aliveIPs[userID][ip] = struct{}{}
		}
	}
	rt.access.Unlock()
}

func (rt *nodeRuntime) aliveUserCount() int {
	rt.access.RLock()
	count := len(rt.aliveIPs)
	rt.access.RUnlock()
	return count
}

func (rt *nodeRuntime) uploadAliveIP(alive map[int]map[string]struct{}) (int, error) {
	a := rt.app
	if len(alive) == 0 {
		return 0, nil
	}
	now := time.Now().Unix()
	ctx, cancel := context.WithTimeout(a.ctx, dbOperationTimeout)
	defer cancel()
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	records := make([]aliveIPRecord, 0, reportBatchSize)
	totalRecords := 0
	for userID, ips := range alive {
		for ip := range ips {
			records = append(records, aliveIPRecord{UserID: userID, IP: ip})
			totalRecords++
			if len(records) == reportBatchSize {
				if err := insertAliveIPBatch(ctx, tx, rt.nodeID, now, records); err != nil {
					return 0, err
				}
				records = records[:0]
			}
		}
	}
	if len(records) > 0 {
		if err := insertAliveIPBatch(ctx, tx, rt.nodeID, now, records); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return totalRecords, nil
}

func (a *App) uploadOnlineLog(nodeID int, onlineUsers int) error {
	now := time.Now().Unix()
	ctx, cancel := context.WithTimeout(a.ctx, dbOperationTimeout)
	defer cancel()
	_, err := a.db.ExecContext(ctx, "INSERT INTO ss_node_online_log (node_id, online_user, log_time) VALUES (?, ?, ?)", nodeID, onlineUsers, now)
	return err
}

func (a *App) uploadNodeInfo(nodeID int, onlineUsers int) error {
	load := readLoadAverage()
	uptime := readUptime()
	now := time.Now().Unix()
	ctx, cancel := context.WithTimeout(a.ctx, dbOperationTimeout)
	defer cancel()
	_, err := a.db.ExecContext(ctx,
		"INSERT INTO ss_node_info (node_id, uptime, `load`, log_time) VALUES (?, ?, ?, ?)",
		nodeID, uptime, load, now,
	)
	return err
}

func (a *App) uploadNodeHeartbeat(nodeID int, now time.Time) error {
	ctx, cancel := context.WithTimeout(a.ctx, dbOperationTimeout)
	defer cancel()
	_, err := a.db.ExecContext(ctx, "UPDATE ss_node SET node_heartbeat = ? WHERE id = ?", now.Unix(), nodeID)
	return err
}

func (rt *nodeRuntime) storeUsers(users []panelUser) {
	userMap := make(map[int]panelUser, len(users))
	disconnected := make(map[int]map[string]struct{})
	for _, user := range users {
		userMap[user.ID] = compactTrafficUser(user)
		blocked := parseIPList(user.DisconnectIP)
		if len(blocked) > 0 {
			disconnected[user.ID] = blocked
		}
	}
	rt.access.Lock()
	for userID, user := range rt.users {
		if _, ok := userMap[userID]; !ok {
			if _, hasTraffic := rt.lastTraffic[userID]; hasTraffic {
				rt.retiringUsers[userID] = user
			}
		}
	}
	for userID := range userMap {
		delete(rt.retiringUsers, userID)
	}
	for userID := range rt.lastTraffic {
		if _, ok := userMap[userID]; ok {
			continue
		}
		if _, ok := rt.retiringUsers[userID]; ok {
			continue
		}
		delete(rt.lastTraffic, userID)
	}
	if len(disconnected) == 0 {
		disconnected = nil
	}
	rt.users = userMap
	rt.disconnected = disconnected
	rt.access.Unlock()
}

func compactTrafficUser(user panelUser) panelUser {
	return panelUser{
		ID:                  user.ID,
		UplinkCounterName:   user.UplinkCounterName,
		DownlinkCounterName: user.DownlinkCounterName,
	}
}

func (rt *nodeRuntime) compactCachedUsers() {
	for i := range rt.cachedUsers {
		rt.cachedUsers[i].Passwd = ""
		rt.cachedUsers[i].DisconnectIP = ""
	}
}

func (rt *nodeRuntime) trafficUsers(users []panelUser) []panelUser {
	if len(rt.retiringUsers) == 0 {
		return users
	}
	byID := make(map[int]panelUser, len(users)+len(rt.retiringUsers))
	for _, user := range users {
		byID[user.ID] = user
	}
	for userID, user := range rt.retiringUsers {
		if _, ok := byID[userID]; !ok {
			byID[userID] = user
		}
	}
	merged := make([]panelUser, 0, len(byID))
	for _, user := range byID {
		merged = append(merged, user)
	}
	sort.Slice(merged, func(i, j int) bool { return merged[i].ID < merged[j].ID })
	return merged
}

func (rt *nodeRuntime) pruneRetiringUsers(activeTraffic map[int]struct{}) {
	for userID := range rt.retiringUsers {
		if _, active := activeTraffic[userID]; active {
			continue
		}
		delete(rt.retiringUsers, userID)
		delete(rt.lastTraffic, userID)
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
		_, _ = fmt.Fprintf(hash, "%d:%s;", user.ID, user.VmessUUID)
	}
	return strconv.FormatUint(hash.Sum64(), 16)
}

func inboundTag(nodeID int) string {
	return mainInboundTagPrefix + strconv.Itoa(nodeID)
}

func sameInboundSettings(a vmessNodeSettings, b vmessNodeSettings) bool {
	return a.Port == b.Port && a.AlterID == b.AlterID && a.Network == b.Network
}

func (u *panelUser) prepareRuntimeFields(nodeID int, multiNode bool) {
	u.VmessEmail = userEmail(nodeID, u.ID, multiNode)
	u.VmessUUID = userUUID(u.ID, u.Passwd)
	u.UplinkCounterName = "user>>>" + u.VmessEmail + ">>>traffic>>>uplink"
	u.DownlinkCounterName = "user>>>" + u.VmessEmail + ">>>traffic>>>downlink"
	u.Passwd = ""
}

func userEmail(nodeID int, userID int, multiNode bool) string {
	if multiNode {
		return "sspanel-node-" + strconv.Itoa(nodeID) + "-user-" + strconv.Itoa(userID)
	}
	return "sspanel-user-" + strconv.Itoa(userID)
}

func userRefFromEmail(email string) (nodeID int, userID int, hasNode bool, ok bool) {
	if strings.HasPrefix(email, "sspanel-node-") {
		rest := strings.TrimPrefix(email, "sspanel-node-")
		parts := strings.SplitN(rest, "-user-", 2)
		if len(parts) != 2 {
			return 0, 0, false, false
		}
		parsedNodeID, err := strconv.Atoi(parts[0])
		if err != nil {
			return 0, 0, false, false
		}
		parsedUserID, err := strconv.Atoi(parts[1])
		if err != nil {
			return 0, 0, false, false
		}
		return parsedNodeID, parsedUserID, true, true
	}
	if strings.HasPrefix(email, "sspanel-user-") {
		parsedUserID, err := strconv.Atoi(strings.TrimPrefix(email, "sspanel-user-"))
		if err != nil {
			return 0, 0, false, false
		}
		return 0, parsedUserID, false, true
	}
	return 0, 0, false, false
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

func updateUserTrafficBatch(ctx context.Context, tx *sql.Tx, now int64, deltas []trafficDelta) error {
	if len(deltas) == 0 {
		return nil
	}
	args := make([]interface{}, 0, 1+len(deltas)*5)
	args = append(args, now)
	var query strings.Builder
	query.Grow(128 + len(deltas)*48)
	query.WriteString("UPDATE `user` SET t = ?, u = u + CASE id ")
	for _, delta := range deltas {
		query.WriteString("WHEN ? THEN ? ")
		args = append(args, delta.UserID, delta.RatedU)
	}
	query.WriteString("ELSE 0 END, d = d + CASE id ")
	for _, delta := range deltas {
		query.WriteString("WHEN ? THEN ? ")
		args = append(args, delta.UserID, delta.RatedD)
	}
	query.WriteString("ELSE 0 END WHERE id IN (")
	for i, delta := range deltas {
		if i > 0 {
			query.WriteByte(',')
		}
		query.WriteByte('?')
		args = append(args, delta.UserID)
	}
	query.WriteByte(')')
	_, err := tx.ExecContext(ctx, query.String(), args...)
	return err
}

func insertTrafficLogBatch(ctx context.Context, tx *sql.Tx, nodeID int, now int64, deltas []trafficDelta) error {
	if len(deltas) == 0 {
		return nil
	}
	args := make([]interface{}, 0, len(deltas)*7)
	var query strings.Builder
	query.Grow(96 + len(deltas)*24)
	query.WriteString("INSERT INTO user_traffic_log (user_id, u, d, node_id, rate, traffic, log_time) VALUES ")
	for i, delta := range deltas {
		if i > 0 {
			query.WriteByte(',')
		}
		query.WriteString("(?, ?, ?, ?, ?, ?, ?)")
		args = append(args, delta.UserID, delta.U, delta.D, nodeID, delta.Rate, formatTraffic(float64(delta.U+delta.D)*delta.Rate), now)
	}
	_, err := tx.ExecContext(ctx, query.String(), args...)
	return err
}

func insertAliveIPBatch(ctx context.Context, tx *sql.Tx, nodeID int, now int64, records []aliveIPRecord) error {
	if len(records) == 0 {
		return nil
	}
	args := make([]interface{}, 0, len(records)*4)
	var query strings.Builder
	query.Grow(64 + len(records)*16)
	query.WriteString("INSERT INTO alive_ip (nodeid, userid, ip, datetime) VALUES ")
	for i, record := range records {
		if i > 0 {
			query.WriteByte(',')
		}
		query.WriteString("(?, ?, ?, ?)")
		args = append(args, nodeID, record.UserID, record.IP, now)
	}
	_, err := tx.ExecContext(ctx, query.String(), args...)
	return err
}

func reportDue(last time.Time, intervalSeconds uint32, now time.Time) bool {
	interval := time.Duration(intervalSeconds) * time.Second
	return last.IsZero() || now.Sub(last) >= interval
}

func markReportSuccess(last *time.Time, now time.Time) {
	*last = now
}

func markReportFailure(last *time.Time, intervalSeconds uint32, now time.Time) {
	retry := time.Duration(reportFailureRetrySeconds) * time.Second
	interval := time.Duration(intervalSeconds) * time.Second
	if retry > interval {
		retry = interval
	}
	*last = now.Add(-interval + retry)
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
