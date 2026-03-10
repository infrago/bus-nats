package bus_nats

import (
	"errors"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/infrago/bus"
	"github.com/infrago/infra"
	"github.com/nats-io/nats.go"
	"github.com/vmihailenco/msgpack/v5"
)

var (
	errNatsInvalidConnection = errors.New("invalid nats connection")
	errNatsAlreadyRunning    = errors.New("nats bus is already running")
	errNatsNotRunning        = errors.New("nats bus is not running")
)

const (
	systemAnnounceTopic    = "announce"
	defaultAnnouncePeriod  = 10 * time.Second
	defaultAnnounceTimeout = 30 * time.Second
	defaultAnnounceJitter  = 2 * time.Second
)

type (
	natsBusDriver struct{}

	natsBusConnection struct {
		mutex   sync.RWMutex
		running bool

		instance *bus.Instance
		setting  natsBusSetting
		client   *nats.Conn

		services map[string]struct{}
		messages map[string]struct{}
		retries  map[string][]time.Duration
		subs     []*nats.Subscription

		identity infra.NodeInfo
		cache    map[string]infra.NodeInfo

		announceInterval time.Duration
		announceJitter   time.Duration
		announceTTL      time.Duration
		done             chan struct{}
		wg               sync.WaitGroup

		stats map[string]*statsEntry
		rnd   *rand.Rand
	}

	natsBusSetting struct {
		URL          string
		Token        string
		Username     string
		Password     string
		QueueGroup   string
		PublishGroup string
		Prefix       string
		Version      string

		AnnounceInterval time.Duration
		AnnounceJitter   time.Duration
		AnnounceTTL      time.Duration
	}

	statsEntry struct {
		name         string
		numRequests  int
		numErrors    int
		totalLatency int64
	}
)

func init() {
	infra.Register("nats", &natsBusDriver{})
}

func (driver *natsBusDriver) Connect(inst *bus.Instance) (bus.Connection, error) {
	setting := natsBusSetting{
		URL:     nats.DefaultURL,
		Version: "1.0.0",
		Prefix:  inst.Config.Prefix,
	}

	if v, ok := inst.Config.Setting["url"].(string); ok && v != "" {
		setting.URL = v
	}
	if v, ok := inst.Config.Setting["server"].(string); ok && v != "" {
		setting.URL = v
	}
	if v, ok := inst.Config.Setting["token"].(string); ok && v != "" {
		setting.Token = v
	}
	if v, ok := inst.Config.Setting["user"].(string); ok && v != "" {
		setting.Username = v
	}
	if v, ok := inst.Config.Setting["username"].(string); ok && v != "" {
		setting.Username = v
	}
	if v, ok := inst.Config.Setting["pass"].(string); ok && v != "" {
		setting.Password = v
	}
	if v, ok := inst.Config.Setting["password"].(string); ok && v != "" {
		setting.Password = v
	}
	if v, ok := inst.Config.Setting["group"].(string); ok && v != "" {
		setting.QueueGroup = v
	}
	if v := strings.TrimSpace(inst.Config.Group); v != "" {
		setting.PublishGroup = v
	} else if v, ok := inst.Config.Setting["role"].(string); ok && strings.TrimSpace(v) != "" {
		setting.PublishGroup = strings.TrimSpace(v)
	} else if v, ok := inst.Config.Setting["profile"].(string); ok && strings.TrimSpace(v) != "" {
		setting.PublishGroup = strings.TrimSpace(v)
	}
	if v, ok := inst.Config.Setting["version"].(string); ok && v != "" {
		setting.Version = v
	}
	setting.AnnounceInterval = parseDurationSetting(inst.Config.Setting["announce"])
	if setting.AnnounceInterval <= 0 {
		setting.AnnounceInterval = defaultAnnouncePeriod
	}
	setting.AnnounceTTL = parseDurationSetting(inst.Config.Setting["announce_ttl"])
	if setting.AnnounceTTL <= 0 {
		setting.AnnounceTTL = defaultAnnounceTimeout
	}
	setting.AnnounceJitter = parseDurationSetting(inst.Config.Setting["jitter"])
	if setting.AnnounceJitter <= 0 {
		// compatibility with older key
		setting.AnnounceJitter = parseDurationSetting(inst.Config.Setting["announce_jitter"])
	}
	if setting.AnnounceJitter <= 0 {
		setting.AnnounceJitter = defaultAnnounceJitter
	}

	id := infra.Identity()
	project := id.Project
	if strings.TrimSpace(project) == "" {
		project = infra.INFRAGO
	}
	node := id.Node
	if strings.TrimSpace(node) == "" {
		node = infra.Generate("node")
	}
	profile := id.Profile
	if strings.TrimSpace(profile) == "" {
		profile = infra.INFRAGO
	}
	role := id.Role
	if strings.TrimSpace(role) == "" {
		role = profile
	}
	return &natsBusConnection{
		instance: inst,
		setting:  setting,
		services: make(map[string]struct{}, 0),
		messages: make(map[string]struct{}, 0),
		retries:  make(map[string][]time.Duration, 0),
		subs:     make([]*nats.Subscription, 0),
		identity: infra.NodeInfo{
			Project: project,
			Node:    node,
			Role:    role,
			Profile: profile,
		},
		cache:            make(map[string]infra.NodeInfo, 0),
		announceInterval: setting.AnnounceInterval,
		announceJitter:   setting.AnnounceJitter,
		announceTTL:      setting.AnnounceTTL,
		done:             make(chan struct{}),
		stats:            make(map[string]*statsEntry, 0),
		rnd:              rand.New(rand.NewSource(time.Now().UnixNano())),
	}, nil
}

func (c *natsBusConnection) RegisterService(subject string, retries []time.Duration) error {
	c.mutex.Lock()
	c.services[subject] = struct{}{}
	c.retries[subject] = append([]time.Duration{}, retries...)
	c.mutex.Unlock()
	return nil
}

func (c *natsBusConnection) RegisterMessage(subject string) error {
	c.mutex.Lock()
	c.messages[subject] = struct{}{}
	c.mutex.Unlock()
	return nil
}

func (c *natsBusConnection) Open() error {
	opts := []nats.Option{}
	if c.setting.Token != "" {
		opts = append(opts, nats.Token(c.setting.Token))
	}
	if c.setting.Username != "" || c.setting.Password != "" {
		opts = append(opts, nats.UserInfo(c.setting.Username, c.setting.Password))
	}

	client, err := nats.Connect(c.setting.URL, opts...)
	if err != nil {
		return err
	}
	c.client = client
	return nil
}

func (c *natsBusConnection) Close() error {
	if c.client != nil {
		c.client.Close()
	}
	return nil
}

func (c *natsBusConnection) Start() error {
	c.mutex.Lock()
	if c.running {
		c.mutex.Unlock()
		return errNatsAlreadyRunning
	}
	if c.client == nil {
		c.mutex.Unlock()
		return errNatsInvalidConnection
	}

	for subject := range c.services {
		callSubject := "call." + subject
		queueSubject := "queue." + subject

		callSub, err := c.client.QueueSubscribe(callSubject, c.queueGroup(callSubject), func(msg *nats.Msg) {
			started := time.Now()
			resp, callErr := c.handleCall(msg.Data)
			if callErr != nil {
				c.recordStats(subject, time.Since(started), callErr)
				return
			}
			_ = msg.Respond(resp)
			c.recordStats(subject, time.Since(started), nil)
		})
		if err != nil {
			c.mutex.Unlock()
			return err
		}
		c.subs = append(c.subs, callSub)

		queueSub, err := c.client.QueueSubscribe(queueSubject, c.queueGroup(queueSubject), func(msg *nats.Msg) {
			started := time.Now()
			attempt := queueAttempt(msg.Header)
			after := queueAfter(msg.Header)
			retries := c.retries[subject]
			if after > 0 {
				now := time.Now()
				target := time.Unix(after, 0)
				if target.After(now) {
					time.Sleep(time.Second)
					_ = c.publishQueue(queueSubject, msg.Data, attempt, target.Sub(now))
					c.recordStats(subject, time.Since(started), nil)
					return
				}
			}

			asyncErr := c.handleServiceAsync(msg.Data, attempt, bus.DispatchFinal(retries, attempt))
			if asyncErr != nil && bus.IsRetryableDispatchError(asyncErr) {
				if delay, ok := bus.DispatchRetryDelay(retries, attempt); ok {
					_ = c.publishQueue(queueSubject, msg.Data, attempt+1, delay)
				}
			}
			c.recordStats(subject, time.Since(started), asyncErr)
		})
		if err != nil {
			c.mutex.Unlock()
			return err
		}
		c.subs = append(c.subs, queueSub)

	}

	for subject := range c.messages {
		eventSubject := "event." + subject
		groupSubject := "publish." + subject

		eventSub, err := c.client.Subscribe(eventSubject, func(msg *nats.Msg) {
			started := time.Now()
			asyncErr := c.handleMessage(msg.Data)
			c.recordStats(subject, time.Since(started), asyncErr)
		})
		if err != nil {
			c.mutex.Unlock()
			return err
		}
		c.subs = append(c.subs, eventSub)

		groupSub, err := c.client.QueueSubscribe(groupSubject, c.publishGroup(groupSubject), func(msg *nats.Msg) {
			started := time.Now()
			asyncErr := c.handleMessage(msg.Data)
			c.recordStats(subject, time.Since(started), asyncErr)
		})
		if err != nil {
			c.mutex.Unlock()
			return err
		}
		c.subs = append(c.subs, groupSub)
	}

	announceSub, err := c.client.Subscribe(c.announceSubject(), func(msg *nats.Msg) {
		c.onAnnounce(msg.Data)
	})
	if err != nil {
		c.mutex.Unlock()
		return err
	}
	c.subs = append(c.subs, announceSub)

	c.running = true
	c.mutex.Unlock()

	c.publishAnnounce()

	c.wg.Add(2)
	go c.announceLoop()
	go c.gcLoop()

	return nil
}

func (c *natsBusConnection) Stop() error {
	c.mutex.Lock()
	if !c.running {
		c.mutex.Unlock()
		return errNatsNotRunning
	}

	subs := c.subs
	c.subs = nil
	done := c.done
	c.running = false
	c.mutex.Unlock()

	c.publishOffline()

	close(done)

	for _, sub := range subs {
		_ = sub.Unsubscribe()
	}

	c.wg.Wait()
	c.mutex.Lock()
	c.done = make(chan struct{})
	c.mutex.Unlock()

	return nil
}

func (c *natsBusConnection) Request(subject string, data []byte, timeout time.Duration) ([]byte, error) {
	if c.client == nil {
		return nil, errNatsInvalidConnection
	}

	msg, err := c.client.Request(subject, data, timeout)
	if err != nil {
		return nil, err
	}
	return msg.Data, nil
}

func (c *natsBusConnection) Publish(subject string, data []byte) error {
	if c.client == nil {
		return errNatsInvalidConnection
	}
	return c.client.Publish(subject, data)
}

func (c *natsBusConnection) Enqueue(subject string, data []byte) error {
	if c.client == nil {
		return errNatsInvalidConnection
	}
	if strings.HasPrefix(subject, "queue.") {
		return c.publishQueue(subject, data, 1, 0)
	}
	return c.client.Publish(subject, data)
}

func (c *natsBusConnection) Stats() []infra.ServiceStats {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	all := make([]infra.ServiceStats, 0, len(c.stats))
	for _, st := range c.stats {
		avg := int64(0)
		if st.numRequests > 0 {
			avg = st.totalLatency / int64(st.numRequests)
		}
		all = append(all, infra.ServiceStats{
			Name:         st.name,
			Version:      c.setting.Version,
			NumRequests:  st.numRequests,
			NumErrors:    st.numErrors,
			TotalLatency: st.totalLatency,
			AvgLatency:   avg,
		})
	}
	return all
}

func (c *natsBusConnection) ListNodes() []infra.NodeInfo {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	now := time.Now().UnixMilli()
	out := make([]infra.NodeInfo, 0, len(c.cache))
	for _, item := range c.cache {
		if c.announceTTL > 0 && now-item.Updated > c.announceTTL.Milliseconds() {
			continue
		}
		item.Services = cloneStrings(item.Services)
		out = append(out, item)
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Project == out[j].Project {
			if out[i].Role == out[j].Role {
				if out[i].Profile == out[j].Profile {
					return out[i].Node < out[j].Node
				}
				return out[i].Profile < out[j].Profile
			}
			return out[i].Role < out[j].Role
		}
		return out[i].Project < out[j].Project
	})
	return out
}

func (c *natsBusConnection) ListServices() []infra.ServiceInfo {
	nodes := c.ListNodes()
	if len(nodes) == 0 {
		return nil
	}
	merged := make(map[string]*infra.ServiceInfo)
	for _, node := range nodes {
		for _, svc := range node.Services {
			svcKey := svc
			info, ok := merged[svcKey]
			if !ok {
				info = &infra.ServiceInfo{Service: svc, Name: svc}
				merged[svcKey] = info
			}
			info.Nodes = append(info.Nodes, infra.ServiceNode{
				Node:    node.Node,
				Role:    node.Role,
				Profile: node.Profile,
			})
			if node.Updated > info.Updated {
				info.Updated = node.Updated
			}
		}
	}

	out := make([]infra.ServiceInfo, 0, len(merged))
	for _, info := range merged {
		sort.Slice(info.Nodes, func(i, j int) bool {
			if info.Nodes[i].Role == info.Nodes[j].Role {
				if info.Nodes[i].Profile == info.Nodes[j].Profile {
					return info.Nodes[i].Node < info.Nodes[j].Node
				}
				return info.Nodes[i].Profile < info.Nodes[j].Profile
			}
			return info.Nodes[i].Role < info.Nodes[j].Role
		})
		info.Instances = len(info.Nodes)
		out = append(out, *info)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Service < out[j].Service
	})
	return out
}

func (c *natsBusConnection) queueGroup(subject string) string {
	if c.setting.QueueGroup != "" {
		return c.setting.QueueGroup + "." + subject
	}
	return subject
}

func (c *natsBusConnection) publishGroup(subject string) string {
	group := strings.TrimSpace(c.setting.PublishGroup)
	if group == "" {
		group = strings.TrimSpace(c.identity.Role)
	}
	if group == "" {
		group = infra.GLOBAL
	}
	return group + "." + subject
}

func (c *natsBusConnection) handleCall(data []byte) ([]byte, error) {
	if c.instance == nil {
		c.instance = &bus.Instance{}
	}
	return c.instance.HandleCall(data)
}

func (c *natsBusConnection) handleServiceAsync(data []byte, attempt int, final bool) error {
	if c.instance == nil {
		c.instance = &bus.Instance{}
	}
	return c.instance.HandleServiceAsync(data, attempt, final)
}

func (c *natsBusConnection) handleMessage(data []byte) error {
	if c.instance == nil {
		c.instance = &bus.Instance{}
	}
	return c.instance.HandleMessage(data)
}

func (c *natsBusConnection) recordStats(subject string, cost time.Duration, err error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	st, ok := c.stats[subject]
	if !ok {
		st = &statsEntry{name: subject}
		c.stats[subject] = st
	}
	st.numRequests++
	st.totalLatency += cost.Milliseconds()
	if err != nil {
		st.numErrors++
	}
}

type announcePayload struct {
	Project  string   `json:"project"`
	Node     string   `json:"node"`
	Role     string   `json:"role"`
	Profile  string   `json:"profile"`
	Services []string `json:"services"`
	Updated  int64    `json:"updated"`
	Online   *bool    `json:"online,omitempty"`
}

func (c *natsBusConnection) announceLoop() {
	defer c.wg.Done()
	for {
		wait := c.nextAnnounceDelay()
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
			c.publishAnnounce()
		case <-c.done:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return
		}
	}
}

func (c *natsBusConnection) gcLoop() {
	defer c.wg.Done()
	interval := c.announceInterval
	if interval <= 0 {
		interval = defaultAnnouncePeriod
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.gcCache()
		case <-c.done:
			return
		}
	}
}

func (c *natsBusConnection) publishAnnounce() {
	c.publishAnnounceState(true)
}

func (c *natsBusConnection) publishOffline() {
	c.publishAnnounceState(false)
}

func (c *natsBusConnection) publishAnnounceState(online bool) {
	if c.client == nil {
		return
	}
	payload := announcePayload{
		Project: c.identity.Project,
		Node:    c.identity.Node,
		Role:    c.identity.Role,
		Profile: c.identity.Profile,
		Updated: time.Now().UnixMilli(),
	}
	if online {
		payload.Services = c.currentServices()
	}
	flag := online
	payload.Online = &flag
	data, err := msgpack.Marshal(payload)
	if err != nil {
		return
	}
	_ = c.client.Publish(c.announceSubject(), data)
	c.onAnnounce(data)
}

func (c *natsBusConnection) onAnnounce(data []byte) {
	var payload announcePayload
	if err := msgpack.Unmarshal(data, &payload); err != nil {
		return
	}
	if strings.TrimSpace(payload.Node) == "" {
		return
	}
	if strings.TrimSpace(payload.Project) == "" {
		payload.Project = infra.INFRAGO
	}
	online := true
	if payload.Online != nil {
		online = *payload.Online
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	key := payload.Project + "|" + payload.Node
	if !online {
		delete(c.cache, key)
		return
	}

	c.cache[key] = infra.NodeInfo{
		Project:  payload.Project,
		Node:     payload.Node,
		Role:     payload.Role,
		Profile:  payload.Profile,
		Services: uniqueStrings(payload.Services),
		Updated:  payload.Updated,
	}
}

func (c *natsBusConnection) gcCache() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.announceTTL <= 0 {
		return
	}
	now := time.Now().UnixMilli()
	for key, item := range c.cache {
		if now-item.Updated > c.announceTTL.Milliseconds() {
			delete(c.cache, key)
		}
	}
}

func (c *natsBusConnection) currentServices() []string {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	names := make([]string, 0, len(c.services))
	for name := range c.services {
		names = append(names, c.serviceName(name))
	}
	sort.Strings(names)
	return names
}

func (c *natsBusConnection) serviceName(subject string) string {
	if c.setting.Prefix == "" {
		return subject
	}
	return strings.TrimPrefix(subject, c.setting.Prefix)
}

func (c *natsBusConnection) announceSubject() string {
	return c.systemSubject(systemAnnounceTopic)
}

func (c *natsBusConnection) systemPrefixValue() string {
	if c.setting.Prefix != "" {
		return strings.TrimSuffix(c.setting.Prefix, ".")
	}
	project := strings.TrimSpace(c.identity.Project)
	if project == "" {
		project = strings.TrimSpace(infra.Identity().Project)
	}
	if project == "" {
		project = infra.INFRAGO
	}
	return project
}

func (c *natsBusConnection) systemSubject(msg string) string {
	return "_" + c.systemPrefixValue() + "." + msg
}

func (c *natsBusConnection) nextAnnounceDelay() time.Duration {
	base := c.announceInterval
	if base <= 0 {
		base = defaultAnnouncePeriod
	}
	jitter := c.announceJitter
	if jitter <= 0 || c.rnd == nil {
		return base
	}

	span := jitter.Milliseconds()
	if span <= 0 {
		return base
	}
	offsetMs := c.rnd.Int63n(span*2+1) - span
	delay := base + time.Duration(offsetMs)*time.Millisecond
	if delay < 100*time.Millisecond {
		delay = 100 * time.Millisecond
	}
	return delay
}

func parseDurationSetting(v any) time.Duration {
	switch vv := v.(type) {
	case time.Duration:
		return vv
	case int:
		return time.Second * time.Duration(vv)
	case int64:
		return time.Second * time.Duration(vv)
	case float64:
		return time.Second * time.Duration(vv)
	case string:
		if d, err := time.ParseDuration(vv); err == nil {
			return d
		}
		if n, err := strconv.Atoi(strings.TrimSpace(vv)); err == nil {
			return time.Second * time.Duration(n)
		}
	}
	return 0
}

func (c *natsBusConnection) publishQueue(subject string, data []byte, attempt int, delay time.Duration) error {
	msg := nats.NewMsg(subject)
	msg.Data = data
	msg.Header = nats.Header{}
	if attempt <= 0 {
		attempt = 1
	}
	msg.Header.Set("attempt", strconv.Itoa(attempt))
	if delay > 0 {
		msg.Header.Set("after", strconv.FormatInt(time.Now().Add(delay).Unix(), 10))
	}
	return c.client.PublishMsg(msg)
}

func queueAttempt(header nats.Header) int {
	if header == nil {
		return 1
	}
	raw := strings.TrimSpace(header.Get("attempt"))
	if raw == "" {
		return 1
	}
	val, err := strconv.Atoi(raw)
	if err != nil || val <= 0 {
		return 1
	}
	return val
}

func queueAfter(header nats.Header) int64 {
	if header == nil {
		return 0
	}
	raw := strings.TrimSpace(header.Get("after"))
	if raw == "" {
		return 0
	}
	val, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0
	}
	return val
}

func uniqueStrings(in []string) []string {
	if len(in) == 0 {
		return []string{}
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, v := range in {
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

func cloneStrings(in []string) []string {
	out := make([]string, len(in))
	copy(out, in)
	return out
}
