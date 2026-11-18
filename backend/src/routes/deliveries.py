from fastapi import APIRouter,Depends,HTTPException
from fastapi.responses import Response,StreamingResponse
from src.config.database import database
from src.controllers.auth import current,admin
from src.models.run import Run,Schedule,Delivery
from src.schemas.requests import ScheduleInput
from src.services.delivery import add_schedule
from src.services.export import csv_text
from src.services.pipeline import iter_part_rows,csv_chunks
from src.utils.serialize import row
router=APIRouter()
@router.get('/schedules')
def schedules(db=Depends(database),u=Depends(current)): return [row(x) for x in db.query(Schedule).all()]
@router.post('/schedules')
def create(b:ScheduleInput,db=Depends(database),u=Depends(admin)): return row(add_schedule(db,u,b))
@router.post('/schedules/{id}/toggle')
def toggle(id:int,db=Depends(database),u=Depends(admin)):
 obj=db.get(Schedule,id)
 if not obj: raise HTTPException(404,'计划不存在')
 obj.enabled=not obj.enabled;db.commit();return row(obj)
@router.get('/deliveries')
def deliveries(db=Depends(database),u=Depends(current)): return [row(x) for x in db.query(Delivery).order_by(Delivery.id.desc()).limit(100).all()]
@router.get('/deliveries/{id}/csv')
def download(id:int,db=Depends(database),u=Depends(current)):
 obj=db.get(Delivery,id)
 if not obj: raise HTTPException(404,'交付不存在')
 result=(obj.payload or {}).get('result') or {}
 if result.get('rows'):
  return Response(csv_text(result),media_type='text/csv',headers={'Content-Disposition':f'attachment; filename="delivery-{id}.csv"'})
 run=db.get(Run,obj.run_id)
 if run and run.status=='completed' and run.result and run.result.get('parts'):
  stream=csv_chunks(iter_part_rows(run.id,run.result['parts']))
  return StreamingResponse(stream,media_type='text/csv',headers={'Content-Disposition':f'attachment; filename="delivery-{id}.csv"','X-Accel-Buffering':'no'})
 raise HTTPException(409,'交付内容不可用')
