#-*- coding:utf-8 -*-
import psutil
import time
import redis
import pytz
from apscheduler.schedulers.background import BackgroundScheduler
import pandas as pd
import numpy as np
#import json
#import requests

cpu_interval = 30
endpoint = "EP1"
YOUHOSTIP = '190.168.16.111'
refresh_interval = 300
#push_interval = 300

codesz = []
#todaystr = time.strftime('%Y%m%d')
#timestr = time.strftime('%Y%m%d %H:%M:%S') 
#datest = int(time.mktime(time.strptime(todaystr,'%Y%m%d')))

def m_agent():
    #payload = []
    _rd = redis.StrictRedis(host=YOUHOSTIP, port=6379, password='', db=0)

    cpu_status = psutil.cpu_times_percent(interval=cpu_interval)
    mem_status = psutil.virtual_memory()
    net_addrs_status = psutil.net_if_addrs()

    lstart = str(net_addrs_status).find('192.168.')
    lend = str(net_addrs_status)[lstart:].find('\', netmask')

    datadict = {"Host":str(net_addrs_status)[lstart:lstart+lend],"Mem_total":round(mem_status.total/1024/1024/1024,1),"ts":int(time.time()),"CPU_up":round(100-cpu_status.idle,1),"Mem_up":mem_status.percent}
    _rd.hmset(endpoint, datadict)
    _rd.expire(endpoint, refresh_interval)
    #r = requests.post(push_url, data=json.dumps(payload))
       
def job1():
    #取得 job1 执行的任务配置 get config for job1
    r1 = redis.StrictRedis(host=YOUHOSTIP, port=6379, password='', db=0, decode_responses=True)
    while True:
        if r1.llen("job1"): 
            try:
                row = tuple(eval(r1.lpop("job1")))
                break
            except:
                print('error tuple count job1')
                r1.setex(endpoint,'{} @{} job1 error'.format(endpoint,time.strftime('%Y%m%d %H:%M:%S'))) 
                time.sleep(1)
        else:
            time.sleep(1)
    #row[0],row[1] 
    #取得相同的工作任务，并加载以上不同的任务配置，执行 get work info, loading job1 config, exec
    r = redis.StrictRedis(host=YOUHOSTIP, port=6379, password='', db=1, decode_responses=True)  
    exec(r.get("job")) 
    #job内配置了未定义的row值,从以上redis队列取得各个电脑的任务范围不同，动态执行
            
def job2():
    r1 = redis.StrictRedis(host=YOUHOSTIP, port=6379, password='', db=0, decode_responses=True)
    while True:
        if r1.llen("job2"): 
            try:
                row = tuple(eval(r1.lpop("job2")))
                break
            except:
                print('error tuple count job2')
                r1.setex(endpoint,'{} @{} job2 error'.format(endpoint,time.strftime('%Y%m%d %H:%M:%S')))
                time.sleep(1)
        else:
            time.sleep(1)
    #row[0],row[1] 
    r = redis.StrictRedis(host=YOUHOSTIP, port=6379, password='', db=2, decode_responses=True)  
    exec(r.get("job")) 

    
def job3():
    r1 = redis.StrictRedis(host=YOUHOSTIP, port=6379, password='', db=0, decode_responses=True)
    while True:
        if r1.llen("job3"): 
            try:
                row = tuple(eval(r1.lpop("job3")))
                break
            except:
                print('error tuple count job3')
                r1.setex(endpoint,'{} @{} job3 error'.format(endpoint,time.strftime('%Y%m%d %H:%M:%S')))
                time.sleep(1)
        else:
            time.sleep(1)
    #row[0],row[1] 
    r = redis.StrictRedis(host=YOUHOSTIP, port=6379, password='', db=3, decode_responses=True)  
    exec(r.get("job")) 
    #job内配置了未定义的row值,从以上redis队列取得各个电脑的任务范围不同
'''
def job4():
    you other jobs...
'''
    
if __name__ == "__main__":
    scheduler = BackgroundScheduler(timezone=pytz.timezone('Asia/Shanghai'))
    scheduler.start()
    scheduler.add_job(m_agent, 'cron', day_of_week='mon-fri', minute = '*/5') #监控在线及状态 5min/次
    scheduler.add_job(job1, 'cron', day_of_week='mon-fri', minute = '*/10') #任务调度 10min/次
    scheduler.add_job(job2, 'cron', day='*', hour = 8 ) #任务调度 8:00am 每天/次
    #scheduler.add_job(job3, 'cron', )
    while True:
        time.sleep(1)
    
