import random
def demo_rows():
 """240条合成演示底稿，seed=42；seed_demo与验证脚本共用。"""
 rng=random.Random(42);rows=[]
 for i in range(240):
  total=rng.randint(600,1050);rows.append({'period':'2026-11-'+str(1+i%20).zfill(2),'department':['试制机A','试制机B','试制机C'][i%3],'start':480,'end':1020,'ideal_seconds':25,'total':total,'good':total-rng.randint(10,60),'downtime':[[540,570],[560,590],[950,970]]})
 return rows
