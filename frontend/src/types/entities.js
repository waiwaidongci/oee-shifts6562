// API实体字段约定：来源{id,natural_key,data}、口径{id,version,data}、报告{id,status,snapshot,checksum,result,approved,progress}、计划{next_at,enabled}。
// 报告快照两种格式：旧版snapshot.sources为内联数组；segments-v1为分段冻结（snapshot.source_count/segments），大行数结果经/runs/{id}/rows分页读取。
export const ENTITY_KEYS=['sources','policies','runs','schedules','deliveries'];
