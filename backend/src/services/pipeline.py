import hashlib,heapq,json,math,os,shutil
from pathlib import Path
from src.config import settings
from src.services.validation import validate_source,validate_policy
from src.services.calculation import shift_row,sort_key,summary_of,METHOD
def canon(obj): return json.dumps(obj,ensure_ascii=False,sort_keys=True,separators=(',',':'))
def run_dir(run_id): return Path(settings.ARTIFACTS_DIR)/'runs'/str(run_id)
def _tmp_dir(run_id): return run_dir(run_id)/'tmp'
def _out_dir(run_id): return run_dir(run_id)/'out'
def _rows_path(run_id,idx): return _tmp_dir(run_id)/f'rows-{idx:05d}.jsonl'
def freeze_sources(dest,records,policy,period):
 """冻结：把来源记录流式写成规范JSONL分段，返回(快照元数据,SHA256)。内存只占单条记录。"""
 validate_policy(policy)
 inp=Path(dest)/'input';inp.mkdir(parents=True,exist_ok=True)
 seg_size=settings.SEGMENT_SIZE
 h=hashlib.sha256();h.update(canon(policy).encode());h.update(b'\n');h.update(period.encode());h.update(b'\n')
 segments=[];count=0;idx=-1;fh=None;tmp=None;seg_sha=None;seg_n=0
 try:
  for rec in records:
   if fh is None:
    idx+=1;tmp=inp/f'.seg-{idx:05d}.tmp';fh=open(tmp,'w',encoding='utf-8');seg_sha=hashlib.sha256();seg_n=0
   line=canon(rec);b=(line+'\n').encode()
   fh.write(line+'\n');seg_sha.update(b);h.update(b);seg_n+=1;count+=1
   if seg_n>=seg_size:
    fh.close();fh=None;os.replace(tmp,inp/f'seg-{idx:05d}.jsonl')
    segments.append({'file':f'input/seg-{idx:05d}.jsonl','count':seg_n,'sha256':seg_sha.hexdigest()})
  if fh is not None:
   fh.close();fh=None;os.replace(tmp,inp/f'seg-{idx:05d}.jsonl')
   segments.append({'file':f'input/seg-{idx:05d}.jsonl','count':seg_n,'sha256':seg_sha.hexdigest()})
 finally:
  if fh is not None: fh.close()
 return {'format':'segments-v1','policy':policy,'period':period,'source_count':count,'seg_size':seg_size,'segments':segments,'frozen':True},h.hexdigest()
def _read_trailer(path,kind,idx):
 """校验分段文件完整性（数据行sha+行数+尾签），损坏或半截文件返回None以便重算。"""
 try:
  h=hashlib.sha256();n=0;last=None
  with open(path,encoding='utf-8') as f:
   for line in f:
    if line.startswith('{"trailer"'): last=line
    else: h.update(line.encode());n+=1
  if last is None: return None
  t=json.loads(last)['trailer']
  if t.get(kind)!=idx or t['count']!=n or t['sha256']!=h.hexdigest(): return None
  return t
 except Exception: return None
def _compute_segment(run_id,idx,seg):
 """核算单个冻结分段：校验冻结sha→逐班计算→段内排序→写带尾签的临时分段（原子替换）。"""
 src=run_dir(run_id)/seg['file']
 tmpd=_tmp_dir(run_id);tmpd.mkdir(parents=True,exist_ok=True)
 tmp=tmpd/f'.rows-{idx:05d}.tmp'
 rows=[];warns=[];planned=[];operating=[];ideal_total=[];ideal_good=[];inh=hashlib.sha256()
 with open(src,encoding='utf-8') as f:
  for line in f:
   inh.update(line.encode())
   s=json.loads(line);validate_source(s)
   row,warn,(p,op,it,ig)=shift_row(s)
   rows.append((sort_key(s),row))
   if warn: warns.append(warn)
   planned.append(p);operating.append(op);ideal_total.append(it);ideal_good.append(ig)
 if inh.hexdigest()!=seg['sha256']: raise ValueError('冻结分段校验失败: '+seg['file'])
 rows.sort(key=lambda kr:kr[0])
 h=hashlib.sha256()
 with open(tmp,'w',encoding='utf-8') as f:
  for key,row in rows:
   line=canon({'k':key,'r':row});f.write(line+'\n');h.update((line+'\n').encode())
  trailer={'segment':idx,'count':len(rows),'sha256':h.hexdigest(),'planned':math.fsum(planned),'operating':math.fsum(operating),'ideal_total':math.fsum(ideal_total),'ideal_good':math.fsum(ideal_good),'warnings':warns}
  f.write(canon({'trailer':trailer})+'\n')
 os.replace(tmp,_rows_path(run_id,idx))
 return trailer
def _iter_tmp_rows(path):
 with open(path,encoding='utf-8') as f:
  for line in f:
   if line.startswith('{"trailer"'): break
   yield json.loads(line)
MERGE_FANIN=200
def _merge_group(run_id,g,paths):
 """超大任务的分组预归并：限制同时打开的文件数，中间分段同样带尾签可校验、可续算。"""
 out=_tmp_dir(run_id)/f'merge-{g:05d}.jsonl'
 if _read_trailer(out,'part',g) is not None: return out
 tmp=_tmp_dir(run_id)/f'.merge-{g:05d}.tmp'
 h=hashlib.sha256();n=0
 with open(tmp,'w',encoding='utf-8') as f:
  for item in heapq.merge(*[_iter_tmp_rows(p) for p in paths],key=lambda d:d['k']):
   line=canon({'k':item['k'],'r':item['r']});f.write(line+'\n');h.update((line+'\n').encode());n+=1
  f.write(canon({'trailer':{'part':g,'count':n,'sha256':h.hexdigest()}})+'\n')
 os.replace(tmp,out)
 return out
def _merge(run_id,snap,trailers,inputs):
 """k路归并已排序临时分段→发布分段（原子替换）；任何失败清理out目录，不暴露半份结果。"""
 outd=_out_dir(run_id)
 if outd.exists(): shutil.rmtree(outd)
 outd.mkdir(parents=True)
 part_size=settings.PART_SIZE
 parts=[];idx=-1;n=0;total=0;fh=None;tmp=None;h=None
 def close_part():
  nonlocal fh,idx,n,tmp,h
  if fh is None: return
  trailer={'part':idx,'count':n,'sha256':h.hexdigest()}
  fh.write(canon({'trailer':trailer})+'\n');fh.close();fh=None
  os.replace(tmp,outd/f'part-{idx:05d}.jsonl')
  parts.append({'file':f'out/part-{idx:05d}.jsonl','count':n,'sha256':h.hexdigest()})
 try:
  streams=[_iter_tmp_rows(p) for p in inputs]
  for item in heapq.merge(*streams,key=lambda d:d['k']):
   if fh is None:
    idx+=1;tmp=outd/f'.part-{idx:05d}.tmp';fh=open(tmp,'w',encoding='utf-8');h=hashlib.sha256();n=0
   line=canon(item['r']);fh.write(line+'\n');h.update((line+'\n').encode());n+=1;total+=1
   if n>=part_size: close_part()
  close_part()
 except Exception:
  if fh is not None: fh.close()
  shutil.rmtree(outd,ignore_errors=True)
  raise
 warns_first=[];warn_count=0;wh=hashlib.sha256()
 with open(outd/'warnings.jsonl','w',encoding='utf-8') as wf:
  for t in trailers:
   for w in t['warnings']:
    line=canon(w);wf.write(line+'\n');wh.update((line+'\n').encode());warn_count+=1
    if len(warns_first)<settings.WARN_INLINE_LIMIT: warns_first.append(w)
 summary=summary_of(math.fsum(t['planned'] for t in trailers),math.fsum(t['operating'] for t in trailers),math.fsum(t['ideal_total'] for t in trailers),math.fsum(t['ideal_good'] for t in trailers))
 manifest={'summary':summary,'row_count':total,'warnings_count':warn_count,'warnings_sha256':wh.hexdigest(),'parts':parts}
 (outd/'result.json').write_text(json.dumps(manifest,ensure_ascii=False),encoding='utf-8')
 return manifest,warns_first
def _verify_published(run_id,manifest):
 """清理临时分段前的可读性校验：发布分段逐段可解析且行数合计一致。"""
 total=0
 for i,part in enumerate(manifest['parts']):
  t=_read_trailer(run_dir(run_id)/part['file'],'part',i)
  if t is None: raise ValueError('发布分段不可读: '+part['file'])
  total+=t['count']
 if total!=manifest['row_count']: raise ValueError('发布行数与清单不一致')
def iter_part_rows(run_id,parts):
 for part in parts:
  with open(run_dir(run_id)/part['file'],encoding='utf-8') as f:
   for line in f:
    if line.startswith('{"trailer"'): break
    yield json.loads(line)
def _first_warnings(run_id,limit):
 out=[]
 try:
  with open(_out_dir(run_id)/'warnings.jsonl',encoding='utf-8') as f:
   for line in f:
    if len(out)>=limit: break
    out.append(json.loads(line))
 except FileNotFoundError: pass
 return out
def _result_from_manifest(run_id,manifest,warns_first):
 result={'summary':manifest['summary'],'method':METHOD,'warnings':warns_first,'warnings_count':manifest['warnings_count'],'row_count':manifest['row_count'],'sorted_by':'设备,日期','parts':manifest['parts'],'rows':None}
 if manifest['row_count']<=settings.INLINE_LIMIT: result['rows']=list(iter_part_rows(run_id,manifest['parts']))
 return result
def calculate_run(run_id,snap,on_progress=None,hook=None):
 """流式核算：逐段续算→归并发布→校验可读→清理临时分段。中断后仅从文件系统恢复，重试不重复计入。"""
 outd=_out_dir(run_id);manifest_path=outd/'result.json'
 if manifest_path.exists():
  try:
   manifest=json.loads(manifest_path.read_text(encoding='utf-8'))
   _verify_published(run_id,manifest)
   return _result_from_manifest(run_id,manifest,_first_warnings(run_id,settings.WARN_INLINE_LIMIT))
  except Exception: pass
 tmpd=_tmp_dir(run_id);tmpd.mkdir(parents=True,exist_ok=True)
 segments=snap['segments'];trailers=[]
 for idx,seg in enumerate(segments):
  if hook: hook('segment',idx)
  t=_read_trailer(_rows_path(run_id,idx),'segment',idx)
  if t is None: t=_compute_segment(run_id,idx,seg)
  trailers.append(t)
  if on_progress: on_progress(idx+1,len(segments))
 if hook: hook('merge',-1)
 inputs=[_rows_path(run_id,i) for i in range(len(segments))]
 if len(inputs)>MERGE_FANIN:
  inputs=[_merge_group(run_id,g,inputs[off:off+MERGE_FANIN]) for g,off in enumerate(range(0,len(inputs),MERGE_FANIN))]
 manifest,warns=_merge(run_id,snap,trailers,inputs)
 _verify_published(run_id,manifest)
 shutil.rmtree(tmpd,ignore_errors=True)
 return _result_from_manifest(run_id,manifest,warns)
def read_rows_page(run_id,result,page,size):
 """分页读取：内联小报告直接切片，大行数报告流式扫描发布分段，内存恒定。"""
 rows=result.get('rows')
 if rows is not None:
  total=len(rows);return rows[(page-1)*size:page*size],total
 total=result.get('row_count',0);start=(page-1)*size;end=start+size;out=[]
 for i,row in enumerate(iter_part_rows(run_id,result.get('parts',[]))):
  if i>=end: break
  if i>=start: out.append(row)
 return out,total
def csv_chunks(rows,keys=None,batch=2000):
 """流式CSV：逐批生成文本块，导出内存恒定，超量分批而非截断。"""
 import csv,io
 from src.services.calculation import ROW_KEYS
 keys=keys or ROW_KEYS
 head=io.StringIO();csv.DictWriter(head,fieldnames=keys).writeheader()
 yield '\ufeff'+head.getvalue()
 buf=io.StringIO();w=csv.DictWriter(buf,fieldnames=keys);n=0
 for row in rows:
  w.writerow({k:row.get(k) for k in keys});n+=1
  if n>=batch:
   yield buf.getvalue();buf=io.StringIO();w=csv.DictWriter(buf,fieldnames=keys);n=0
 if n: yield buf.getvalue()
def cleanup_tmp(run_id): shutil.rmtree(_tmp_dir(run_id),ignore_errors=True)
def cleanup_orphans(valid_ids,min_age_seconds=600):
 """清理无对应报告记录的产物目录（含冻结暂存目录）；按目录及一级子项最新mtime判断，避免与进行中的提交竞争。"""
 import time
 root=Path(settings.ARTIFACTS_DIR)/'runs'
 if not root.exists(): return
 now=time.time()
 for d in root.iterdir():
  try:
   if not d.is_dir(): continue
   fresh=max([d.stat().st_mtime]+[p.stat().st_mtime for p in d.iterdir()])
   if now-fresh<min_age_seconds: continue
   if d.name.startswith('.staging-') or not d.name.isdigit() or int(d.name) not in valid_ids: shutil.rmtree(d,ignore_errors=True)
  except OSError: pass
