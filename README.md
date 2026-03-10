# bus-nats

`bus-nats` 是 `github.com/infrago/bus` 的**nats 驱动**。

## 包定位

- 类型：驱动
- 作用：把 `bus` 模块的统一接口落到 `nats` 后端实现

## 快速接入

```go
import (
    _ "github.com/infrago/bus"
    _ "github.com/infrago/bus-nats"
)
```

```toml
[bus]
driver = "nats"
```

## `setting` 专用配置项

配置位置：`[bus].setting`

- `url`
- `server`
- `token`
- `user`
- `username`
- `pass`
- `password`
- `group`
- `role`
- `profile`（兼容旧配置，未设置 `role` 时可继续使用）
- `version`
- `announce`
- `announce_ttl`
- `jitter`
- `announce_jitter`

## 说明

- `setting` 仅对当前驱动生效，不同驱动键名可能不同
- 连接失败时优先核对 `setting` 中 host/port/认证/超时等参数
