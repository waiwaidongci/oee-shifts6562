# 002 流式核算（百万班次）

## 结构变更

`oeeruns` 新增可空列：`progress JSON NULL`（分段进度展示，非恢复依据）。新库由 `create_all` 一次建齐；已有库由 `src/config/migrate.py:ensure_schema` 在 backend/worker 启动时自动 `ALTER TABLE` 补齐（MySQL），无需手工执行，升级前仍建议先备份。

## 产物布局（共享卷 report-files → /app/artifacts）

```
runs/{run_id}/input/seg-00000.jsonl   冻结分段：规范JSONL（排序键、紧凑分隔符），逐行SHA256入总校验
runs/{run_id}/tmp/rows-00000.jsonl    核算临时分段：段内已排序{k,key,r,row}行+尾签trailer
runs/{run_id}/tmp/merge-00000.jsonl   超200段时的分组预归并（同带尾签）
runs/{run_id}/out/part-00000.jsonl    已发布逐班行分段（全局按设备、日期等内容键有序）+尾签
runs/{run_id}/out/warnings.jsonl      全部警示（结果JSON仅内联前1000条并记总数）
runs/{run_id}/out/result.json         发布清单：summary、row_count、parts及各自SHA256
```

尾签 trailer 记录 segment/part 序号、行数、数据行SHA256及分段小计（planned/operating/ideal_total/ideal_good，math.fsum）。所有分段先写 `.tmp` 再 `os.replace` 原子落位。

## 恢复协议

- 恢复只认文件系统：临时分段尾签校验（行数+SHA256）通过即视为已完成，跳过重算；损坏或半截文件自动重算并原子覆盖。`progress` 列仅用于页面展示，写分段后未记进度也能恢复。
- 小计由尾签重建后 fsum 汇总，重试不重复计入；已完成任务（out/result.json 校验通过）直接返回，不重算。
- 归并失败会整体删除 out/ 半成品；报告 status 不到 completed、result 不落库，行接口与CSV均返回409，不暴露半份结果。
- 发布清单逐段校验可读后才删除 tmp/ 临时分段；input/ 冻结分段随报告保留，作为快照凭证。
- worker 启动及每60秒对账：status 为 queued/running 的报告重新入队续算；清理超10分钟的无属主产物目录（含冻结暂存 .staging-*）。

## 快照格式

新报告 `snapshot.format='segments-v1'`，含 policy、period、source_count、seg_size、segments（文件/行数/SHA256）；`checksum` 为口径+期间+逐行规范字节的SHA256。旧版内联 `snapshot.sources` 报告与旧交付保持原样可读，导出路径自动分流。
