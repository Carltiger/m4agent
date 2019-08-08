# -*- coding: utf-8 -*-
import time
import redis
def post_job():
    pool = redis.ConnectionPool(host=YOUHOSTIP, port=6379, password='', db=1)
    r = redis.StrictRedis(connection_pool=pool)
    # 以下codelsit[row[0]:row[1]]存在于配置中取出，即任务分配的范围 config, 而此计划被设定执行的时间是10min/次
    data = '''
    for code in codelsit[row[0]:row[1]]: 
        try:
            pass
        except:
            pass
    ''' 
    r.setex("job",data,1800)
    
    pool = redis.ConnectionPool(host=YOUHOSTIP, port=6379, password='', db=2)
    r = redis.StrictRedis(connection_pool=pool)
    # 以下codelsit[row[0]:row[1]]存在于配置中取出，即任务分配的范围 config, 而此计划被设定执行的时间是30min/次
    data = '''
    for code in codelsit[row[0]:row[1]]: 
        pass
    ''' 
    r.setex("job",data,1800)
    
    pool = redis.ConnectionPool(host=YOUHOSTIP, port=6379, password='', db=3)
    r = redis.StrictRedis(connection_pool=pool)
    # 以下codelsit[row[0]:row[1]]存在于配置中取出，即任务分配的范围 config, 而此计划被设定执行的时间是每天/次
    data = '''
    for code in codelsit[row[0]:row[1]]: 
        pass
    ''' 
    r.setex("job",data,1800)
 
if __name__ == "__main__":
    post_job()
