# 网络质量演练系统

第一版是纯 Go 进程内虚拟链路：A、B 两个端点的数据只经过模拟器的虚拟队列，不创建 socket、代理或网卡规则，也不会接管电脑的真实网络。

## 能力

- 两个方向独立配置延迟、带宽字节/秒、丢包率和队列字节容量。
- 按入队报文的实际字节数占用队列；容量不足立即记录 queue_full。
- 带宽决定序列化耗时，随后再叠加传播延迟；同方向保持 FIFO。
- 动态 update 只影响之后入队的报文，已排队或在途报文保持原计划，不会被提前到过去。
- 固定 seed 使用确定性 PRNG，重复同一 JSON 场景会得到相同顺序、时间、状态和丢失原因。
- close 丢弃尚未交付的报文，记录 closed_while_waiting，并结束虚拟读等待。

## 多跳拓扑（version 2）

- 报文沿 paths 逐跳转发，每一跳的序列化和传播都从该跳的真实事件时刻起算；无论一次推进 5 秒还是分多步推进，交付时刻与事件序列完全一致。
- update_link 只影响之后进入该链路的报文；已经在途或已排队序列化的报文保持原计划。链路可用共享带宽，或用 bandwidth_a_to_b / bandwidth_b_to_a、loss_a_to_b / loss_b_to_a 配置方向不对称。
- update_path 只影响尚未进入链路的报文；在途报文到达交换节点后按新路径后缀接续，无可用下一跳时记录 link_down。
- 输出的 decisions 可通过 WithRecordedDecisions 回放：中途保存后恢复到新实例重放，交付时刻、内容与事件顺序不变。
- 每个报文片段都有明确去向记录：queued、serializing、hop_arrived、delivered，或带原因的 dropped（random_loss、queue_full、link_down、packet_too_large 等）。

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
