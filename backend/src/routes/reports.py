from fastapi import APIRouter,Depends,HTTPException
from fastapi.responses import Response,StreamingResponse
from src.config.database import database
from src.config.settings import ROWS_PAGE_MAX
from src.controllers.auth import current,editor,admin
from src.models.run import Run
from src.schemas.requests import RunInput,ReviewInput
from src.services.report import submit,approve
from src.services.pipeline import read_rows_page,iter_part_rows,csv_chunks
from src.utils.serialize import row
router=APIRouter()
@router.get('/runs')
def runs(db=Depends(database),u=Depends(current)): return [row(x) for x in db.query(Run).order_by(Run.id.desc()).all()]
@router.post('/runs')
def create(b:RunInput,db=Depends(database),u=Depends(editor)): return row(submit(db,u,b))
@router.get('/runs/{id}')
def detail(id:int,db=Depends(database),u=Depends(current)):
 obj=db.get(Run,id)
 if not obj: raise HTTPException(404,'报告不存在')
 return row(obj)
@router.post('/runs/{id}/approve')
def review(id:int,b:ReviewInput,db=Depends(database),u=Depends(admin)): return row(approve(db,u,id,b.note))
@router.get('/runs/{id}/rows')
def rows(id:int,page:int=1,size:int=200,db=Depends(database),u=Depends(current)):
 obj=db.get(Run,id)
 if not obj: raise HTTPException(404,'报告不存在')
 if obj.status!='completed' or not obj.result: raise HTTPException(409,'报告尚未完成')
 page=max(1,page);size=min(ROWS_PAGE_MAX,max(1,size))
 data,total=read_rows_page(obj.id,obj.result,page,size)
 return {'rows':data,'page':page,'size':size,'total':total,'pages':max(1,-(-total//size))}
@router.get('/runs/{id}/csv')
def download(id:int,db=Depends(database),u=Depends(current)):
 obj=db.get(Run,id)
 if not obj or obj.status!='completed' or not obj.result: raise HTTPException(409,'报告尚未完成')
 if u.role=='viewer' and not obj.approved: raise HTTPException(403,'业务阅读者仅可下载已复核报告')
 result=obj.result
 if result.get('rows') is not None:
  if not result['rows']: raise HTTPException(409,'尚无结果')
  stream=csv_chunks(iter(result['rows']))
 elif result.get('parts'): stream=csv_chunks(iter_part_rows(obj.id,result['parts']))
 else: raise HTTPException(409,'尚无结果')
 return StreamingResponse(stream,media_type='text/csv',headers={'Content-Disposition':f'attachment; filename="report-{id}.csv"','X-Accel-Buffering':'no'})
