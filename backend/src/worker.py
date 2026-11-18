import time,traceback
from pathlib import Path
import redis
from src.config.database import Base,engine,Session
from src.config.settings import REDIS_URL
from src.config.migrate import ensure_schema
from src.models import identity,source,run
from src.models.run import Run
from src.services.calculation import calculate
from src.services.pipeline import calculate_run,cleanup_tmp,cleanup_orphans
from src.services.delivery import tick
from src.services.audit import log
Base.metadata.create_all(engine)
ensure_schema()
r=redis.Redis.from_url(REDIS_URL)
def reconcile():
 """把排队中/计算中（含中断）的报告重新入队，并清理无属主产物目录。"""
 with Session() as db:
  ids=[x.id for x in db.query(Run.id).filter(Run.status.in_(['queued','running'])).all()]
  for i in ids: r.rpush('report-jobs',i)
  cleanup_orphans({x.id for x in db.query(Run.id).all()})
def process(item):
 run_id=int(item)
 with Session() as db:
  job=db.get(Run,run_id)
  if not job or job.status not in ('queued','running'): return
  job.status='running';db.commit()
  try:
   snap=job.snapshot
   if isinstance(snap,dict) and snap.get('format')=='segments-v1':
    def progress(done,total):
     job.progress={'phase':'核算','segments_done':done,'segments_total':total,'updated':time.time()};db.commit()
    result=calculate_run(job.id,snap,on_progress=progress)
   else: result=calculate(snap['sources'],snap['policy'])
   job.result=result;job.status='completed';job.error=None
   job.progress={'phase':'完成','segments_done':len(snap.get('segments',[])) if isinstance(snap,dict) else 0,'segments_total':len(snap.get('segments',[])) if isinstance(snap,dict) else 0,'updated':time.time()}
   log(db,'worker','run.completed',run_id);db.commit()
  except Exception as exc:
   db.rollback()
   job=db.get(Run,run_id)
   if job:
    job.status='failed';job.error=str(exc);log(db,'worker','run.failed',str(exc)[:200]);db.commit()
   cleanup_tmp(run_id)
   traceback.print_exc()
reconcile()
last=time.time()
while True:
 Path('/tmp/worker-heartbeat').write_text(str(time.time()))
 try:
  item=r.blpop('report-jobs',timeout=5)
  if item: process(item[1])
  if time.time()-last>60: reconcile();last=time.time()
  with Session() as db: tick(db)
 except Exception: traceback.print_exc()
 time.sleep(1)
