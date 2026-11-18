"""规模与中断验证：百万班次流式OEE核算。
用法：
  容器内  docker compose exec -T backend python -m scripts.verify_scale
  本地    DATABASE_URL=sqlite:// JWT_SECRET=x PROJECT_JSON=$PWD/backend/project.json ARTIFACTS_DIR=/tmp/oee-verify python -m scripts.verify_scale
可用 VERIFY_N 调整规模（默认1000000），VERIFY_KEEP=1 保留产物目录。
验证项：
  1. 规模与内存：冻结/核算/导出三阶段峰值内存均低于128MB，行数、排序、汇总与独立参照一致。
  2. 乱序稳定：同一数据两种输入顺序，发布逐班行字节级一致（按设备、日期稳定输出）。
  3. 中断恢复：分段中断、半截分段、归并中断均可续算，重试不重复计入，失败不暴露半份结果。
  4. 冻结隔离：冻结后底稿增改删不影响该次输入。
  5. 兼容：小报告结果与旧算法一致，CSV导出与旧版字节级一致。
"""
import csv,hashlib,json,math,os,random,resource,shutil,tempfile,time,tracemalloc
from pathlib import Path
os.environ.setdefault('ARTIFACTS_DIR',tempfile.mkdtemp(prefix='oee-verify-'))
from src.config import settings
from src.services.calculation import calculate,shift_row,summary_of
from src.services.pipeline import freeze_sources,calculate_run,iter_part_rows,csv_chunks,run_dir
from src.services.export import csv_text
from src.services.validation import validate_source
LIMIT=128*1024*1024
N=int(os.getenv('VERIFY_N','1000000'))
POLICY={'version':'OEE-验证','definition':'实际运行时间=班次分钟-合并裁剪停机；性能=理想产出时间/运行时间'}
def gen(n,seed):
 rng=random.Random(seed)
 for _ in range(n):
  total=rng.randint(600,1050);start=rng.choice([0,480,960])
  yield {'period':f'2026-{rng.randint(1,12):02d}-{rng.randint(1,28):02d}','department':rng.choice(['试制机A','试制机B','试制机C','试制机D']),'start':start,'end':start+rng.choice([480,540,600]),'ideal_seconds':rng.choice([20,25,30]),'total':total,'good':total-rng.randint(10,60),'downtime':[[start+60,start+90],[start+80,start+120],[start+400,start+420]]}
def reference_summary(records,chunk):
 """独立参照：流式分块fsum（与管线公开的分段算术一致），不经过任何分段文件。"""
 acc=[[],[],[],[]];totals=[[],[],[],[]];n=0
 for s in records:
  validate_source(s);_,_,vals=shift_row(s)
  for i,v in enumerate(vals): acc[i].append(v)
  n+=1
  if n%chunk==0:
   for i in range(4): totals[i].append(math.fsum(acc[i]));acc[i]=[]
 for i in range(4): totals[i].append(math.fsum(acc[i]))
 return summary_of(*[math.fsum(t) for t in totals])
def measured(fn):
 tracemalloc.start();t0=time.time()
 try: out=fn();peak=tracemalloc.get_traced_memory()[1]
 finally: tracemalloc.stop()
 return out,peak,time.time()-t0
def check(name,cond,detail=''):
 print(('  ✔ ' if cond else '  ✘ ')+name+((' · '+detail) if detail else ''))
 if not cond: raise SystemExit('验证失败: '+name)
def manifest_of(run_id): return json.loads((run_dir(run_id)/'out'/'result.json').read_text(encoding='utf-8'))
def close(a,b): return all(abs(a[k]-b[k])<=1e-9*max(1,abs(b[k])) for k in b)
def warnings_of(run_id):
 with open(run_dir(run_id)/'out'/'warnings.jsonl',encoding='utf-8') as f: return sorted(json.loads(x) for x in f if x.strip())
def phase(title): print('\n== '+title)
def main():
 root=Path(settings.ARTIFACTS_DIR);print(f'产物目录 {root} · 规模 {N} 班 · 内存上限 128MB(tracemalloc)')
 phase('1. 规模与内存：冻结→核算→导出')
 rid=900001
 (snap,chk),peak1,t1=measured(lambda:freeze_sources(root/'runs'/str(rid),gen(N,42),POLICY,'全部'))
 check('冻结完成',snap['source_count']==N and len(snap['segments'])==math.ceil(N/settings.SEGMENT_SIZE),f"{len(snap['segments'])}个分段 · {t1:.1f}s")
 check('冻结内存<128MB',peak1<LIMIT,f'峰值{peak1/1048576:.1f}MB')
 result,peak2,t2=measured(lambda:calculate_run(rid,snap))
 check('核算完成',result['row_count']==N,f"{len(result['parts'])}个发布分段 · {t2:.1f}s")
 check('核算内存<128MB',peak2<LIMIT,f'峰值{peak2/1048576:.1f}MB')
 ref=reference_summary(gen(N,42),snap['seg_size'])
 check('汇总与独立参照一致',result['summary']==ref,json.dumps({k:round(v,6) for k,v in result['summary'].items()},ensure_ascii=False))
 def export():
  out=root/'export.csv'
  with open(out,'w',encoding='utf-8') as f:
   for chunk in csv_chunks(iter_part_rows(rid,result['parts'])): f.write(chunk)
  return out
 out,peak3,t3=measured(export)
 check('导出内存<128MB',peak3<LIMIT,f'峰值{peak3/1048576:.1f}MB · {t3:.1f}s')
 lines=0;prev=None;ordered=True
 with open(out,encoding='utf-8-sig',newline='') as f:
  for row in csv.reader(f):
   lines+=1
   if lines==1: continue
   key=(row[1],row[0])
   if prev and key<prev: ordered=False
   prev=key
 check('CSV逐批读回且按设备日期有序',lines==N+1 and ordered,f'{lines-1}行数据')
 if N>settings.INLINE_LIMIT: check('大行数结果不内联',result['rows'] is None and result['row_count']==N)
 else: check('小结果内联',result['rows'] is not None)
 phase('2. 乱序输入稳定输出')
 data=list(gen(100000,7));shuffled=data[:];random.Random(8).shuffle(shuffled)
 ids=[]
 for i,records in enumerate((data,shuffled)):
  r=900010+i;sp,_=freeze_sources(root/'runs'/str(r),iter(records),POLICY,'全部');calculate_run(r,sp);ids.append(r)
 m1,m2=(manifest_of(r) for r in ids)
 check('逐班发布行字节级一致',m1['parts']==m2['parts'] and m1['row_count']==m2['row_count'],f"行数{m1['row_count']}")
 check('汇总数值一致(1e-9)',close(m1['summary'],m2['summary']))
 check('警示集合一致',warnings_of(ids[0])==warnings_of(ids[1]))
 phase('3. 中断恢复与重试幂等')
 n3=250000
 ridA=900020;spA,_=freeze_sources(root/'runs'/str(ridA),gen(n3,11),POLICY,'全部');calculate_run(ridA,spA);mA=manifest_of(ridA)
 ridB=900021;spB,_=freeze_sources(root/'runs'/str(ridB),gen(n3,11),POLICY,'全部')
 def crash(event,idx):
  if event=='segment' and idx==7: raise RuntimeError('模拟断电：写完第7段后、记录进度前中断')
 try: calculate_run(ridB,spB,hook=crash);raise SystemExit('应抛出模拟中断')
 except RuntimeError: pass
 check('中断后无半份发布结果',not (run_dir(ridB)/'out'/'result.json').exists())
 broken=run_dir(ridB)/'tmp'/'rows-00003.jsonl'
 with open(broken,'r+b') as f:
  size=f.seek(0,2);f.truncate(size//2)
 calculate_run(ridB,spB);mB=manifest_of(ridB)
 check('分段中断+半截分段后续算一致',mA==mB,'进度仅从分段文件恢复，重试不重复计入')
 ridC=900022;spC,_=freeze_sources(root/'runs'/str(ridC),gen(n3,11),POLICY,'全部')
 def crash_merge(event,idx):
  if event=='merge': raise RuntimeError('模拟归并期断电')
 try: calculate_run(ridC,spC,hook=crash_merge);raise SystemExit('应抛出模拟中断')
 except RuntimeError: pass
 stray=run_dir(ridC)/'out';stray.mkdir(parents=True,exist_ok=True);(stray/'part-00000.jsonl').write_text('{"半截":true}\n',encoding='utf-8')
 calculate_run(ridC,spC);mC=manifest_of(ridC)
 check('归并中断+残留半份文件后恢复一致',mA==mC)
 calculate_run(ridC,spC);mD=manifest_of(ridC)
 check('已完成任务重试幂等',mA==mD and not (run_dir(ridC)/'tmp').exists(),'发布校验通过后才清理临时分段')
 phase('4. 冻结后底稿增改删不影响该次输入')
 base=list(gen(3000,21));frozen_ref=reference_summary(iter(base),settings.SEGMENT_SIZE)
 ridE=900030;spE,_=freeze_sources(root/'runs'/str(ridE),iter(base),POLICY,'全部')
 base.append(next(gen(1,99)));base.pop(0);base[0]['total']=99999
 resE=calculate_run(ridE,spE)
 check('冻结后增改删不影响该次输入',resE['summary']==frozen_ref and resE['row_count']==3000)
 phase('5. 小报告与旧行为兼容')
 from scripts.demo_data import demo_rows
 demo=demo_rows();legacy=calculate(demo,POLICY)
 ridF=900040;spF,_=freeze_sources(root/'runs'/str(ridF),iter(demo),POLICY,'全部');new=calculate_run(ridF,spF)
 key=lambda r:json.dumps(r,ensure_ascii=False,sort_keys=True)
 check('逐班行与旧算法一致(集合)',sorted(new['rows'],key=key)==sorted(legacy['rows'],key=key))
 check('逐班行按设备日期排序',[(r['设备'],r['日期']) for r in new['rows']]==sorted((r['设备'],r['日期']) for r in new['rows']))
 check('警示与口径说明一致',new['warnings']==legacy['warnings'] and new['method']==legacy['method'])
 check('汇总数值一致(1e-9)',close(new['summary'],legacy['summary']))
 check('小报告内联行保留',new['rows'] is not None)
 check('CSV导出与旧版字节级一致',''.join(csv_chunks(iter(legacy['rows'])))==csv_text(legacy))
 warn_src=[{'period':'2026-11-01','department':'试制机Z','start':0,'end':100,'ideal_seconds':60,'total':200,'good':190,'downtime':[]}]
 wl=calculate(warn_src,POLICY);ridG=900041;spG,_=freeze_sources(root/'runs'/str(ridG),iter(warn_src),POLICY,'全部');wn=calculate_run(ridG,spG)
 check('性能>1警示保留不截断',wn['warnings']==wl['warnings'] and wn['warnings_count']==1)
 rss=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/1024
 print(f'\n全部验证通过。进程峰值RSS {rss:.0f}MB · 产物目录 {root}')
 if os.getenv('VERIFY_KEEP')!='1': shutil.rmtree(root,ignore_errors=True)
if __name__=='__main__': main()
