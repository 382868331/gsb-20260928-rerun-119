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

## 动态路由（routes）

- 配置 routes 后替代静态 paths：每个节点按 routes[节点][目的地] 的候选链路顺序选择下一跳，第一条当前 up 的链路胜出；方向不对称带宽照常生效。
- 每次选择都记录一条 route_decision：时间、报文 seq、所在节点、目的地、选中的链路，以及选择时掌握的候选链路事实（up、带宽）。
- 链路在一跳中途中断时，已在该跳传出的片段以 link_down 结束，不会搬到另一条路，恢复后也不会被再次算作新数据；尚未进入该跳的片段在到达交换节点时按当时的路由表选择新路线。
- update_route 只影响尚未进入下一跳的报文；已在序列化或在途的报文保持原跳。比较每条 serializing 事件的 link 字段即可看出某次改路影响了哪些具体片段。
- 候选链路全部 down 时以 no_route 明确停止；路由表形成回环时按已访问节点集合检测，以 routing_loop 明确停止。两种情况都不会继续推进事件。

## 快照分叉与比较

- Snapshot 保存暂停点的完整状态（含 RNG、在途报文、事件与路由决定历史），可序列化为 JSON；RestoreNetwork 恢复后继续推进，交付时刻、事件与路由决定与不中断的运行完全一致。
- 从同一快照恢复两份实例即得到两套独立路由决定：分别施加不同的 update_route / update_link 后推进，用 Summarize 汇总各目的地的交付、重复片段、丢弃原因与仍在途范围，再用 CompareSummaries 输出两个分支的差异。

## 运行

```bash
go test ./...
go run ./cmd/network-drill -file examples/queue.json
go run ./cmd/network-drill -file examples/reroute.json
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
- version 2 场景使用 network_actions，支持 send、update_link、link_down、link_up、update_path、update_route、close；routes 配置动态路由表，输出附带 route_decisions。

send 可设置 size 生成零值字节，或设置 data 发送文本；二者不能同时设置。输出 JSON 中的时间均为虚拟纳秒。
