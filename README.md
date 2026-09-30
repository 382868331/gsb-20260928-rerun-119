# 网络质量演练系统

纯 Go 进程内虚拟网络：A、B 两端点的数据只经过模拟器的离散事件队列，不创建 socket、代理或网卡规则，也不接管真实网络。

## 能力

- 多段链路路径：每条路径是有序的命名段列表，逐段排队、序列化、传播后再进入下一段。
- 共享瓶颈：同一段两个方向共享一个 FIFO 队列和字节容量，序列化按方向各自串行。
- 方向不对称：每段可单独为反向设置带宽与丢包率（bandwidth_bps_reverse、loss_rate_reverse）。
- 途中规则变化：
  - update 只改段配置，已排队/在途报文保留原序列化与到达计划，不会被挪回过去。
  - reroute 只替换报文尚未进入的区段；报文当前所在段及之前的段不可撤回。
  - break/recover 模拟短时断路与恢复；断路时排队和在途报文记 link_down，恢复后新报文正常通行。
- 慢接收方：接收端可配置入站缓冲字节与排空速率；缓冲满时记 receiver_full，排空完成才算交付。
- 字节核对：每个 queued 事件带 segment、location 与 occupancy_bytes，可核对共享队列与接收缓冲占用。
- 到达顺序：离散事件堆按时间、入队顺序确定排序；同刻断路/关闭的丢包按端点与序号稳定排序。
- 随机决策保存与重放：每次丢包判定写入输出 decisions（含随机数 draw 与分支）；把该列表放回场景的 replay 字段重放，事件逐条一致，不一致直接报错。
- 关闭去向：close 为所有未完成报文（排队、在途、接收缓冲中）记录 closed_while_waiting 与位置，并结束虚拟读等待。

## 运行

```bash
go test ./...
go run ./cmd/network-drill -file examples/queue.json       # 旧版单队列场景
go run ./cmd/network-drill -file examples/multipath.json   # 多链路/断路/换路/慢接收方
```

## 场景格式

旧版字段（a_to_b、b_to_a、扁平 update）继续支持。新版多链路场景使用：

- segments：命名段映射。
  - latency_ms：传播延迟。
  - bandwidth_bps：正向带宽（字节/秒）。
  - bandwidth_bps_reverse：可选，反向带宽覆盖。
  - loss_rate / loss_rate_reverse：正向 / 反向入段丢包概率。
  - queue_bytes：两方向共享的队列字节容量。
- routes："a"、"b" 两个方向的初始有序段列表。
- receivers：可选，端点的 buffer_bytes 与 drain_rate_bps（0 表示无限、即时交付）。
- replay：可选，上次运行保存的随机决策列表。
- actions：按 time_ms 排序执行。
  - send：from、seq、size 或 data（二者互斥）。
  - update：多链路模式下配合 segment 更新该段配置。
  - reroute：from 加新的完整 route，只影响尚未进入相应区段的报文。
  - break / recover：segment 短时断路与恢复。
  - close：结束并为未完成报文记录明确去向。

输出 JSON：seed、replayed、events、decisions。事件时间均为虚拟纳秒；events[].location 取 queue、in_flight、receiver，丢失原因包括 queue_full、random_loss、link_down、receiver_full、closed_while_waiting、packet_too_large、empty_packet、no_route。
