package bus_nats

import (
	"errors"
	"sync"
	"time"

	"github.com/infrago/bus"
	"github.com/nats-io/nats.go"
)

var (
	errInvalidConnection = errors.New("Invalid bus connection.")
	errAlreadyRunning    = errors.New("Nats bus is already running.")
	errNotRunning        = errors.New("Nats bus is not running.")
)

type (
	natsBusDriver  struct{}
	natsBusService struct {
		Name  string
		Group string
	}
	natsBusConnect struct {
		mutex   sync.RWMutex
		running bool
		actives int64

		instance *bus.Instance
		setting  natsBusSetting

		client *nats.Conn

		services map[string]natsBusService
		subs     map[string]*nats.Subscription
	}

	natsBusSetting struct {
		Url      string
		Username string
		Password string
	}
)

// 连接
func (driver *natsBusDriver) Connect(inst *bus.Instance) (bus.Connect, error) {
	//获取配置信息
	setting := natsBusSetting{
		Url: nats.DefaultURL,
	}

	if vv, ok := inst.Config.Setting["url"].(string); ok {
		setting.Url = vv
	}
	if vv, ok := inst.Config.Setting["server"].(string); ok {
		setting.Url = vv
	}

	if vv, ok := inst.Config.Setting["user"].(string); ok {
		setting.Username = vv
	}
	if vv, ok := inst.Config.Setting["username"].(string); ok {
		setting.Username = vv
	}
	if vv, ok := inst.Config.Setting["pass"].(string); ok {
		setting.Password = vv
	}
	if vv, ok := inst.Config.Setting["password"].(string); ok {
		setting.Password = vv
	}

	return &natsBusConnect{
		instance: inst, setting: setting,
		services: make(map[string]natsBusService, 0),
		subs:     make(map[string]*nats.Subscription, 0),
	}, nil
}

// 打开连接
func (this *natsBusConnect) Open() error {
	opts := []nats.Option{}
	if this.setting.Username != "" && this.setting.Password != "" {
		opts = append(opts, nats.UserInfo(this.setting.Username, this.setting.Password))
	}
	client, err := nats.Connect(this.setting.Url, opts...)
	if err != nil {
		return err
	}

	this.client = client

	return nil
}
func (this *natsBusConnect) Health() (bus.Health, error) {
	this.mutex.RLock()
	defer this.mutex.RUnlock()
	return bus.Health{Workload: this.actives}, nil
}

// 关闭连接
func (this *natsBusConnect) Close() error {
	if this.client != nil {
		this.client.Close()
	}
	return nil
}

// Rregister 注册
func (this *natsBusConnect) Register(name string) error {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	this.services[name] = natsBusService{name, name}
	return nil
}

// 开始
func (this *natsBusConnect) Start() error {
	if this.running {
		return errAlreadyRunning
	}

	nc := this.client

	//监听
	for subKey, subVal := range this.services {
		key := subKey
		sub, err := nc.QueueSubscribe(key, subVal.Group, func(msg *nats.Msg) {
			this.instance.Serve(key, msg.Data, func(data []byte, err error) {
				if msg.Reply != "" {
					if err == nil && data != nil {
						msg.Respond(data)
					} else {
						//返回空数据
						msg.Respond(make([]byte, 0))
					}
				}
			})
		})

		if err != nil {
			this.Close()
			return err
		}

		//记录sub
		this.subs[key] = sub

	}

	this.running = true
	return nil
}

// 停止订阅
func (this *natsBusConnect) Stop() error {
	if false == this.running {
		return errNotRunning
	}

	for _, sub := range this.subs {
		sub.Unsubscribe()
	}

	this.running = false
	return nil
}

func (this *natsBusConnect) Request(name string, data []byte, timeout time.Duration) ([]byte, error) {
	if this.client == nil {
		return nil, errInvalidConnection
	}

	nc := this.client

	reply, err := nc.Request(name, data, timeout)
	if err != nil {
		return nil, err
	}

	return reply.Data, nil
}
