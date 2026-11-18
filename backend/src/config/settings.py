import os,json
from pathlib import Path
CONFIG=json.loads(Path(os.getenv('PROJECT_JSON','/app/project.json')).read_text())
DATABASE_URL=os.environ['DATABASE_URL']
REDIS_URL=os.getenv('REDIS_URL','redis://redis:6379/0')
SECRET=os.environ['JWT_SECRET']
# 流式核算：冻结/核算/导出均按分段处理，单段远低于128MB内存上限
ARTIFACTS_DIR=os.getenv('ARTIFACTS_DIR','/app/artifacts')
SEGMENT_SIZE=int(os.getenv('OEE_SEGMENT_SIZE','10000'))
PART_SIZE=int(os.getenv('OEE_PART_SIZE','50000'))
INLINE_LIMIT=int(os.getenv('OEE_INLINE_LIMIT','5000'))
WARN_INLINE_LIMIT=int(os.getenv('OEE_WARN_INLINE_LIMIT','1000'))
ROWS_PAGE_MAX=int(os.getenv('OEE_ROWS_PAGE_MAX','1000'))
FREEZE_BATCH=int(os.getenv('OEE_FREEZE_BATCH','2000'))
