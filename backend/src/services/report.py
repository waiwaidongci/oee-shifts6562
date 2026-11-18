import shutil,time,uuid
from pathlib import Path
from fastapi import HTTPException
from src.models.source import Source,Policy
from src.models.run import Run
from src.services.audit import log
from src.services.pipeline import freeze_sources,run_dir
from src.config.settings import REDIS_URL,ARTIFACTS_DIR,FREEZE_BATCH
import redis
def iter_sources(db,period):
 """按键集分页流式读取底稿，冻结阶段内存恒定。"""
 last=0
 while True:
  batch=db.query(Source).filter(Source.id>last).order_by(Source.id).limit(FREEZE_BATCH).all()
  if not batch: break
  last=batch[-1].id
  for x in batch:
   if period=='全部' or x.data.get('period')==period: yield x.data
def submit(db,user,body):
 policy=db.get(Policy,body.policy_id)
 if not policy: raise HTTPException(404,'口径版本不存在')
 staging=Path(ARTIFACTS_DIR)/'runs'/f'.staging-{uuid.uuid4().hex}'
 try: snapshot,checksum=freeze_sources(staging,iter_sources(db,body.period),policy.data,body.period)
 except Exception:
  shutil.rmtree(staging,ignore_errors=True);raise
 if not snapshot['source_count']:
  shutil.rmtree(staging,ignore_errors=True);raise HTTPException(422,'所选期间没有底稿')
 obj=Run(name=body.name,status='queued',snapshot=snapshot,checksum=checksum,created=time.time(),approved=False)
 db.add(obj);db.flush()
 staging.rename(run_dir(obj.id))
 log(db,user.email,'run.queue',body.name);db.commit()
 redis.Redis.from_url(REDIS_URL).rpush('report-jobs',obj.id)
 return obj
def approve(db,user,id,note):
 obj=db.get(Run,id)
 if not obj: raise HTTPException(404,'报告不存在')
 if obj.status!='completed': raise HTTPException(409,'仅可复核已完成报告')
 if obj.approved: raise HTTPException(409,'报告已经复核')
 obj.approved=True;obj.review=note;log(db,user.email,'run.approve',id);db.commit();return obj
