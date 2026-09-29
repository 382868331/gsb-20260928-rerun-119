# netdrill — 网络质量演练系统（第一版）

在进程内部建立一条**虚拟双向链路**，两个本地端点（`A`、`B`）写出的数据依次经过
可配置的「延迟 → 带宽串行化 → 固定种子丢包 → 字节队列」过程。全程不创建 socket、
不修改路由或网卡，不接管电脑的真实网络。

## 能力

- **可配置规则**：每个方向独立的 `Latency`、`BandwidthBps`、`QueueBytes`、`Loss`（`Config`）。
- **按实际字节计费**：传输耗时 `ceil(字节数*8 / 带宽bps)`，串行发送；后一个包必须等前一个包发完。
- **队列耗尽有明确结果**：入队时若在队字节超过 `QueueBytes`，该包记录 `queue_full` 丢弃事件，
  `Write` 返回可 `errors.Is(err, netdrill.ErrQueueFull)` 识别的临时 `net.Error`。
- **动态改规则不追溯**：到达时刻在入队瞬间冻结。改配置只影响之后入队的包，
  已排队的包不会被「瞬间挪到过去」。
- **关闭即结束等待**：`Sim.Close()` 后仍在等待的包记录 `link_closed` 丢弃原因，
  阻塞的 `Read`/`Write` 立即返回 `net.ErrClosed`。
- **固定种子重放**：`SimClock` 手动驱动虚拟时间，`Replay` 在同一线程同步触发发送与到达，
  不受真实 goroutine 调度影响。同种子 + 同脚本 ⇒ 到达顺序、时间戳、丢失原因逐字节一致。
- **流量记录**：`Event`（enqueue/deliver/drop，含字节数、虚拟时间、`DropReason`），可导出/读回 JSON Lines。
- 端点实现 `net.Conn`（数据报式读边界、读写超时、虚拟地址），可直接喂给普通传输代码。

## 快速开始

```go
clk := netdrill.NewSimClock()
sim, _ := netdrill.NewSim(netdrill.Config{
    Latency:      100 * time.Millisecond,
    BandwidthBps: 800,  // 100 字节/秒
    QueueBytes:   250,
    Loss:         0.1,
}, 42, clk) // 固定种子

a, b := sim.Endpoints()
events := netdrill.Replay(sim, clk, []netdrill.Step{
    {Offset: 0,   From: "A", Payload: []byte("hello")},
    {Offset: 50*time.Millisecond, From: "B", Payload: []byte("ack")},
})
// events 可直接断言：Type / Time / Bytes / Reason
```

需要在包真正到达时被 `Read` 消费，可用 `ReplayAsync`（固定 tick 推进虚拟时钟）。
需要墙钟实时演练时，构造传 `netdrill.WallClock{}` 即可。

运行演示：

```sh
go run ./cmd/netdrill-demo
```

运行测试：

```sh
go test -race ./...
```

## 丢包原因

| Reason | 含义 |
| --- | --- |
| `random_loss` | 入队时由固定种子 RNG 抽中 |
| `queue_full` | 入队瞬间在队字节超过队列容量 |
| `link_closed` | 关闭演练时包仍在等待，未到达 |
