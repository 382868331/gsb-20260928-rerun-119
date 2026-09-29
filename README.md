# 网络质量演练系统

第一版是纯 Go 进程内虚拟链路：A、B 两个端点的数据只经过模拟器的虚拟队列，不创建 socket、代理或网卡规则，也不会接管电脑的真实网络。

## 能力

- 两个方向独立配置延迟、带宽字节/秒、丢包率和队列字节容量。
- 按入队报文的实际字节数占用队列；容量不足立即记录 queue_full。
- 带宽决定序列化耗时，随后再叠加传播延迟；同方向保持 FIFO。
- 动态 update 只影响之后入队的报文，已排队或在途报文保持原计划，不会被提前到过去。
- 固定 seed 使用确定性 PRNG，重复同一 JSON 场景会得到相同顺序、时间、状态和丢失原因。
- close 丢弃尚未交付的报文，记录 closed_while_waiting，并结束虚拟读等待。

## 运行

```bash
go test ./...
go run ./cmd/network-drill -file examples/queue.json
```

## 场景格式

- seed：随机数种子。
- duration_ms：最终推进到的虚拟时间。
- a_to_b、b_to_a：方向配置。
- latency_ms：传播延迟。
- bandwidth_bps：带宽，单位为字节/秒。
- loss_rate：0 到 1 的入队丢包概率。
- queue_bytes：发送端队列容量，按实际报文字节计算。
- actions：按 time_ms 排序执行，支持 send、update、close。

send 可设置 size 生成零值字节，或设置 data 发送文本；二者不能同时设置。输出 JSON 中的时间均为虚拟纳秒。
