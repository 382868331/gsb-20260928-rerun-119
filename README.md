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

## 动态选路与分岔比较（version 2 跟进）

- 路由在每次转发时根据已发生的链路状态选择下一跳：配置路径优先，下一跳链路中断或路径没有覆盖当前节点时，在仍然可用的链路上搜索一条不重复经过已访节点的替代路线。
- 每次选择都记录 RouteDecision：决定时刻、所在节点、选中的链路与下一节点、当时已知中断的链路清单，以及当时所有候选出链路的 up/down 状态与该方向带宽快照；选不出路时记录停止原因。记录是不可变快照，之后的带宽变化或链路恢复只影响新决策，查看旧记录始终读到该次决策自己的事实。
- 一跳中途链路中断：已经在该跳上传出的片段直接记 link_down，不搬到别的路，恢复后也不会被当作新数据重发；尚未进入该跳的片段在到达交换节点时按当时状态另选路线。
- 循环路径在 UpdatePath 时被 ErrRoutingLoop 拒绝，到不了目的地的路径被 ErrNoRoute 拒绝；运行中报文若被要求回到已访节点则记 routing_loop 丢弃，无可用下一跳记 no_route，事件推进都会明确停止。
- Fork 从同一暂停点克隆出两套独立网络（含在途片段、链路状态、随机流与全部记录），可以分别施加不同路由决定，再比较交付内容、重复片段与 InFlight 的在途范围。

### 演练场景（link/routing_test.go）

同一人工流量（方向不对称带宽、l2 中断 50ms~400ms、恢复后 l2 带宽 2000→4000 字节/秒）下：

- seq 1、3、4 到达 r1 时 l2 正中断，RouteDecision 记录 down=[l2] 并改道 l4；
- seq 5 在恢复与带宽变更后到达，走 l2 并按新带宽在 655ms 交付；
- 大步（一次推进 3 秒）、细步（1 毫秒步进）与保存 decisions 后换种子重开，三者的事件、路由记录与交付逐条一致；
- Fork 分岔中，已进 l1 的 seq 1 在两个未来都不被动，只有尚未入链的 seq 3 分别走 l1 与 l3。

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
