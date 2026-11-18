import logging
from sqlalchemy import text
from src.config.database import engine
def ensure_schema():
 """轻量演进：为已有数据库补充流式核算字段；新库由create_all一次建齐。"""
 try:
  with engine.connect() as c:
   cols={r[1] for r in c.execute(text('SHOW COLUMNS FROM oeeruns'))}
   if 'progress' not in cols:
    c.execute(text('ALTER TABLE oeeruns ADD COLUMN progress JSON NULL'));c.commit()
 except Exception: logging.exception('ensure_schema跳过（表不存在或非MySQL时由create_all负责）')
