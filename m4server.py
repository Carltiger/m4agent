# -*- coding: utf-8 -*-
import psutil
import time
import redis
import pytz
import pandas as pd
import numpy as np
import re
import subprocess
from apscheduler.schedulers.background import BackgroundScheduler

def job_renew():
    r1 = redis.StrictRedis(host='', port=6379, password='')
    r1.flushall()
cpu_interval = 30
endpoint = "SERVER"
refresh_interval = 300

#监控多台终端redisdb 状态
def redis_agent():
    _redis_cli = r'C:\ProgramData\Redis\redis-cli'
    mem_regex = re.compile(r'(\w+):([0-9]+\.?[0-9]*\w)\r') #regex = re.compile(r'(\w+):([0-9]+\.?[0-9]*)\r')
    key_regex = re.compile(r'(\w+):(\w+\=?[0-9]*)\,')
    
    hosts = []
    port = '6379'
    passwd = ''
    
    for host in hosts:
        _cmd = '%s -h %s -p %s -a %s info' % (_redis_cli, host, port, passwd)
        try:
            child = subprocess.Popen(_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            info = child.stdout.read().decode()
        except:
            info = ''
        memused = ''
        memmax = ''
        connected = ''
        memdict = dict(mem_regex.findall(info))
        if memdict:
            if 'used_memory_rss_human' in memdict.keys():
                memused = memdict['used_memory_rss_human'] 
            if 'maxmemory_human' in memdict.keys():    
                memmax = memdict['maxmemory_human']
            if 'connected_clients' in memdict.keys(): 
                connected = memdict['connected_clients']
        db0key = ''
        db1key = '' 
        db2key = '' 
        db3key = ''          
        keydict = dict(key_regex.findall(info))
        if keydict:
            if 'db0' in keydict.keys():
                db0key = keydict['db0']
            if 'db1' in keydict.keys():
                db1key = keydict['db1']
            if 'db2' in keydict.keys():
                db2key = keydict['db2']
            if 'db3' in keydict.keys():
                db3key = keydict['db3']
        print('DB:{} mem:{} used:{} conn:{} db0:{} db1:{} db2:{} db3:{}'.format(host,memmax,memused,connected,db0key,db1key,db2key,db3key))        
    print('##'*40)
    
def job_agent():
    payload = []
    eps = ['EP1','EP2','EP3','EP4','EP5','EP6','EP7','EP8','EP9','EP10','EP11','EP12','EP13','EP14','EP15','EP16','EP17','EP18','EP19','EP20']
    _rd = redis.StrictRedis(host=YOUHOSTIP, port=6379, password='', db=0, decode_responses=True)
    #datadict = {"Host":str(net_addrs_status)[lstart:lstart+lend],"Mem_total":round(mem_status.total/1024/1024/1024,1),"ts":int(time.time()),"CPU_up":round(100-cpu_status.idle,1),"Mem_up":mem_status.percent}
    for row in eps:
        try:
            datadict = _rd.hmget(row,['Host','Mem_total','ts','CPU_up','Mem_up'])
            if None not in datadict:
                payload.append(datadict)
        except:
            print('@{} {} is not on line'.format(time.strftime('%Y%m%d %H:%M:%S'),row))
    if len(payload) == 0:
        print('@{} abvalable mechines nums:0'.format(time.strftime('%Y%m%d %H:%M:%S')))
    elif len(payload) <= 5:  #小于5台终端在线时不分配运行任务         
        data = pd.DataFrame(payload)
        print('@{} abvalable mechines nums:{}'.format(time.strftime('%Y%m%d %H:%M:%S'),len(data)))
        print(data)
        print('##'*40)
    elif len(payload) > 5:  
        #配置job1 ,codesz为job1配置执行总范围，大致平均分配每台终端
        codesz = []
        data = pd.DataFrame(payload,columns = ['Host','Mem_total','ts','CPU_up','Mem_up'])
        print('@{} abvalable mechines nums:{}'.format(time.strftime('%Y%m%d %H:%M:%S'),len(data)))
        print(data)
        print('##'*40)
        r1 = redis.StrictRedis(host=YOUHOSTIP, port=6379, password='', db=1)
        pipe = r1.pipeline(transaction=False)
        batch = int(len(codesz)/len(data))+1
        for n in range(len(data)-1):
            pipe.rpush("job1",str((batch*n,batch*(n+1))))
        pipe.rpush("jobl",str((batch*(len(data)-1),len(codesz))))
        pipe.execute() 
        
        #配置job2 ,codesz为job2配置执行总范围，大致平均分配每台终端
        codesz = []
        r2 = redis.StrictRedis(host=YOUHOSTIP, port=6379, password='', db=2)
        pipe = r2.pipeline(transaction=False)
        batch = int(len(codesz)/len(data))+1
        for n in range(len(data)-1):
            pipe.rpush("job2",str((batch*n,batch*(n+1))))
        pipe.rpush("job2",str((batch*(len(data)-1),len(codesz))))
        pipe.execute() 
        
        #配置job3 ,codesz为job3配置执行总范围，大致平均分配每台终端 。。。。
        codesz = []
        r2 = redis.StrictRedis(host=YOUHOSTIP, port=6379, password='', db=3)
        pipe = r2.pipeline(transaction=False)
        batch = int(len(codesz)/len(data))+1
        for n in range(len(data)-1):
            pipe.rpush("job3",str((batch*n,batch*(n+1))))
        pipe.rpush("job3",str((batch*(len(data)-1),len(codesz))))
        pipe.execute() 
        
if __name__ == "__main__":
    scheduler = BackgroundScheduler(timezone=pytz.timezone('Asia/Shanghai'))
    scheduler.start()
    scheduler.add_job(job_renew, 'cron', day = '*', hour=6, minute = 30) #每天6:30清空redis
    scheduler.add_job(redis_agent, 'cron', day_of_week='mon-fri', hour='9-15', minute = '*/10') #每10min/次运行redis监控
    scheduler.add_job(job_agent, 'cron', 'cron', day_of_week='mon-fri', hour='9-15', minute = '*/10') #每10min/次同步一次job分配，按需
    #scheduler.add_job(job1, 'date', run_date='2019-08-08' ) #或只同步一次job分配，按需
    while True:
        time.sleep(1)  
        
