#encoding:utf-8
import pymssql as py
import MySQLdb as my
from time import sleep
import time
import multiprocessing
import redis
import sys
import paramiko as pk

user_serveradmin=''
passwd_serveradmin=''

user_datatransfer_control=''
passwd_datatransfer_control=''

########返回当前时间
def getdate():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

########强制启动redis server
def start_redis_server(redis_host,redis_port):
    redis_reconn_times=0
    redis_reconn_flag=1
    ssh = pk.SSHClient()
    ssh.set_missing_host_key_policy(pk.AutoAddPolicy())
    while True:
        if redis_reconn_times>10:
            return -1,'Retry 10 times,can not start redis-server @'+redis_host
        try:
            redis_conn=redis.Redis(host=redis_host,port=redis_port,socket_timeout=2)
            dbsize=redis_conn.dbsize()
        except Exception,e:
            redis_reconn_times+=1
            redis_reconn_flag=0
        if redis_reconn_flag==0:
            try:
                ssh.connect(hostname=redis_host, port=1022, username='', timeout=1, password='')
                ssh.exec_command('sudo redis-server /usr/local/redis/etc/redis.conf')
                ssh.close()
            except Exception,e:
                print 'Can not connect the server ',redis_host
        else:
            del redis_conn
            return 1,'Start redis server successfully!'
        redis_reconn_flag=1
#########创建redis server 连接
def get_redis_conn(redis_host,redis_port):
    redis_status_flag=1
    try:
        redis_conn=redis.Redis(host=redis_host,port=redis_port,socket_timeout=2)
        dbsize=redis_conn.dbsize()
    except Exception,e:
        redis_status_flag=0
    if redis_status_flag==0:
        redis_status_flag=1
        msg=start_redis_server(redis_host,redis_port)
        if msg[0]==1:
            redis_conn=redis.Redis(host=redis_host,port=redis_port,socket_timeout=2)
            return 1,redis_conn
    else:
        return 1,redis_conn

#######创建数据库连接
def get_database_connections(host,port,db,user,password,dbtype):
    if dbtype.lower()=='mysql':
        try:
            myconn=my.connect(host=host,port=port,db=db,user=user,passwd=password,charset='utf8')
        except Exception,e:
            print Exception,e
            return -1,''
        return 1,myconn
    elif dbtype.lower()=='sqlserver':
        try:
            mssqlconn=py.connect(host=host,port=port,database=db,user=user,password=password,charset='utf8')
        except Exception,e:
            print Exception,e
            return -1, ''
        return 1,mssqlconn
    else:
        return -1,''

########检测当前延迟情况############################################################
# 0. 连接 master server
# 1. 获取slave 服务器列表
# 2. 轮训slave 服务器，获取延迟
# 3. 计算平均延迟
# 4.根据平均延迟情况，调整数据导入频率，主要是通过设置redis中sleep seconds值实现
#  default：
#  delay 阈值，当延迟超过60s 后对迁移速度进行调整
#  sleep seconds 默认100 ms，当小于100ms时，置为100ms
####################################################################################
def check_replication_delay(task_id,dst_serverinfo,redis_serverinfo):
    dst_server_host = dst_serverinfo[0]
    dst_server_port = dst_serverinfo[1]
    dst_server_db = dst_serverinfo[2]
    redis_host=redis_serverinfo[0]
    redis_port=redis_serverinfo[1]
    ips=[]
########创建mysql数据导入目的服务器连接
    myconn_flag, myconn = get_database_connections(host=dst_server_host, port=dst_server_port, db=dst_server_db,user=user_serveradmin, password=passwd_serveradmin, dbtype='mysql')
    if myconn_flag == -1:
        print getdate(), 'task_id', task_id,'connects', dst_server_host, ' failed!\n'
        sys.exit()
    cur_myconn = myconn.cursor()
######获取slave  host列表
    cur_myconn.execute("select host from information_schema.processlist  where user='usvr_replication'")
    tt=cur_myconn.fetchall()
    cur_myconn.close()
    myconn.close()
    for item in tt:
        ips.append((item[0].split(':')[0], int(dst_server_port)))
    del tt
    seconds_behind_master_total=0
    slave_total=0
###### 实时获取slave 延迟情况
    while True:
        redis_flag, redis_conn = get_redis_conn(redis_host, redis_port)
        seconds_behind_master_total=0
        slave_total=0
        sleep(5)
        try:
            rank_finished=redis_conn.hget('datatransfer_control','rank_finished')
        except Exception,e:
            redis_flag, redis_conn = get_redis_conn(redis_host, redis_port)
            rank_finished=0
        if rank_finished=='1':
            break
        try:
            sleep_seconds_str=redis_conn.hget('datatransfer_control','sleep_seconds')
        except Exception,e:
            redis_flag, redis_conn = get_redis_conn(redis_host, redis_port)
            sleep_second_str='1'
        if (not sleep_seconds_str) or (sleep_seconds_str=='0'):
            sleep_seconds=0.05
        else:
            sleep_seconds=float(sleep_seconds_str)
        for item in ips:
            myconn_flag, myconn = get_database_connections(host=item[0], port=item[1],db=dst_server_db,user=user_serveradmin, password=passwd_serveradmin,dbtype='mysql')
            if myconn_flag == -1:
                print 'item[0]',item[0]
                print 'item[1]',item[1]
                print getdate(), 'task_id', task_id, 'connects slave ',item[0],' failed!\n'
                seconds_behind_master_total +=1
            else:
                cur_myconn = myconn.cursor()
                dict_cursor=myconn.cursor(my.cursors.DictCursor)
                dict_cursor.execute("show slave status")
                tt=dict_cursor.fetchone()
                if tt:
                    if tt['Seconds_Behind_Master']:
                        seconds_behind_master_total+=tt['Seconds_Behind_Master']
                    else:
                        seconds_behind_master_total+=1
            slave_total+=1
        if slave_total==0:
            slave_total=1
        seconds_behind_master=seconds_behind_master_total*1.0/slave_total
########平均延迟超过60s，进行速度调整
        print 'seconds_behind_master',seconds_behind_master
        if (seconds_behind_master>60):
            redis_conn.hset('datatransfer_control','sleep_seconds',sleep_seconds*2)
        if (seconds_behind_master<60):
            if sleep_seconds>0.05:
                redis_conn.hset('datatransfer_control', 'sleep_seconds', sleep_seconds /2.0)
            elif sleep_seconds<0.05:
                redis_conn.hset('datatransfer_control', 'sleep_seconds', 0.05)
        del redis_conn
        del redis_flag

###########################每个进程单独取数，然后插入mysql 表，针对大表，超大表##########################################################################################################################################
# 0.获取数据源服务器列表，目标服务器列表
# 1.获取取数sql语句，数据插入sql语句，主键名称，分表数量等基础信息
# 2.根据配置信息，重写sql，重新生成sql语句等
# 3.进行数据迁移
#########################################################################################################################################################################################################################
def insert_mysql(task_id,src_serverinfo,dst_serverinfo,redis_serverinfo,env,rank_id,last_rank,mssql_tbname,mysql_tbname,thread_id,sql_getdata,sql_input,pk_name,shard_num,start_id,end_id=''):
    src_server_host=src_serverinfo[0]
    src_server_port=src_serverinfo[1]
    src_server_db=src_serverinfo[2]
    dst_server_host=dst_serverinfo[0]
    dst_server_port=dst_serverinfo[1]
    dst_server_db=dst_serverinfo[2]
    redis_host=redis_serverinfo[0]
    redis_port=redis_serverinfo[1]
   # print 'rank_id',rank_id,'thead_id',thread_id,'Enter the insert_mysql function!'
   # sleep(2)
######创建transfer 服务器数据转移记录连接
    myconn_datatransfer_control_flag,myconn_datatransfer_control=get_database_connections(host='', port=, db='',user=user_serveradmin, password=passwd_serveradmin,dbtype='mysql')
    if myconn_datatransfer_control_flag==-1:
        print getdate(),'task_id',task_id,'thread_id',thread_id,'connects datatransfer_control fails'
        sys.exit()
    cur_myconn_datatransfer=myconn_datatransfer_control.cursor()
#######创建 redis server 连接##########
    redis_flag,redis_conn=get_redis_conn(redis_host,redis_port)
########创建mysql数据导入目的服务器连接
    myconn_flag,myconn=get_database_connections(host=dst_server_host,port=dst_server_port,db=dst_server_db,user=user_serveradmin,password=passwd_serveradmin,dbtype='mysql')
    if myconn_flag==-1:
        print getdate(),'task_id',task_id,'thread_id',thread_id,'connects',dst_server_host,'failed!\n'
        sys.exit()
    cur_myconn=myconn.cursor()
#######创建mssql数据源连接
    mssql_flag,mssql=get_database_connections(host=src_server_host,port=src_server_port,db=src_server_db,user='',password='',dbtype='sqlserver')
    if mssql_flag==-1:
        print getdate(),'task_id',task_id,'thread_id',thread_id, 'connects ',src_server_host,'failed!'
        sys.exit()
    cur_mssql=mssql.cursor()
#####重新生成sql_getdata语句
#####判断是否是最后一批取数据
    if last_rank:
        sql=sql_getdata+" where "+mssql_tbname+'.'+pk_name+">="+str(start_id) +" and "+mssql_tbname+'.'+pk_name+"<="+str(end_id)
    else:
        sql=sql_getdata+" where "+mssql_tbname+'.'+pk_name+">="+str(start_id)+" and "+mssql_tbname+'.'+pk_name+" < "+str(end_id)
    print 'rank_id',rank_id,'thread_id',thread_id,'sql_getdata:',sql
####取数
    cur_mssql.execute(sql)
    tt=cur_mssql.fetchall()
    cur_mssql.close()
    mssql.close()
    start_time=getdate()
    len_tt=len(tt)
####检测分片数量
    if shard_num==1:     ####不分表
        k=0
        while True:
            sleep_seconds_str=redis_conn.hget('datatransfer_control','sleep_seconds')
            if (not sleep_seconds_str) or(sleep_seconds_str=='0'):
                sleep_seconds=0.1
            else:
                sleep_seconds=float(sleep_seconds_str)
            tmp_inputdata1 = tt[k:k + 500]
            tmp_inputdata2 = tt[k + 500:k + 502]
            if not tmp_inputdata1:
                break
            if len(tmp_inputdata2) == 1:
                tmp_inputdata1 = tt[k:]
                k = k + 1
            try:
                cur_myconn.executemany(sql_input, tmp_inputdata1)
                myconn.commit()
            except Exception, e:
                print '\033[1;31;40m'
                print thread_id,Exception, e
                print '\033[0m'
                sleep(5)
            del tmp_inputdata1
            k += 500
            try:
                sleep(sleep_seconds)
            except Exception,e:
                print Exception,e
        del k
        try:
            cur_myconn_datatransfer.execute(
                "insert into transfer_records(task_id,mssql_tbname,mysql_tbname,thread_id,start_id,end_id,rows,start_time,end_time) values(%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (task_id,env+'_'+mssql_tbname, env+'_'+mysql_tbname,str(rank_id)+":"+str(thread_id), start_id, end_id, len_tt, start_time, getdate()))
            myconn_datatransfer_control.commit()
        except Exception, e:
            print '\033[1;31;40m'
            print 'myconn_datatransfer', Exception, e
            print '\033[0m'
        myconn.commit()
    else:                                    #分表，重新生产sql_input 语句
        sql1=sql_input[:sql_input.find('(')-1]
        sql2=sql_input[sql_input.find('('):]
        result_classfied = []
        for i in range (shard_num):
            tt_result=[]
            result_classfied.append(tt_result)
            del tt_result
        for item in tt:
            k=item[0]%shard_num
            result_classfied[k].append(item)
        del tt
        for i in range(len(result_classfied)):
            print getdate(),"task_id_"+str(task_id)+" thread_"+str(thread_id)+": now do the data transfer for table "+sql1.upper().replace('INSERT INTO ','')+str(i)
            sql_input=sql1+str(i)+sql2
            k=0
            while True:
                sleep_seconds_str = redis_conn.hget('datatransfer_control', 'sleep_seconds')
                if (not sleep_seconds_str) or (sleep_seconds_str == '0'):
                    sleep_seconds = 0.1
                else:
                    sleep_seconds = float(sleep_seconds_str)
                tmp_inputdata1 = result_classfied[i][k:k + 500]
                tmp_inputdata2 = result_classfied[i][k + 500:k + 502]
                if not tmp_inputdata1:
                    break
                if len(tmp_inputdata2)==1:
                    tmp_inputdata1 = result_classfied[i][k:]
                    k+=1
                try:
                    cur_myconn.executemany(sql_input, tmp_inputdata1)
                    myconn.commit()
                except Exception, e:
                    print '\033[1;31;40m'
                    print thread_id,Exception, e
                    print '\033[0m'
                    sleep(5)
                del tmp_inputdata1
                k += 500
                sleep(sleep_seconds)
            del k
            #####写入数据迁移记录
            try:
                cur_myconn_datatransfer.execute(
                    "insert into transfer_records(task_id,mssql_tbname,mysql_tbname,thread_id,start_id,end_id,rows,start_time,end_time) values(%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (task_id,env+'_'+mssql_tbname,env+'_'+mysql_tbname.replace("0",str(i)),str(rank_id)+":"+str(thread_id), start_id, end_id, len(result_classfied[i]), start_time, getdate()))
                myconn_datatransfer_control.commit()
            except Exception,e:
                print '\033[1;31;40m'
                print 'myconn_datatransfer', Exception, e
                print '\033[0m'
            myconn.commit()
####关闭cursor，关闭连接
    cur_myconn_datatransfer.close()
    myconn_datatransfer_control.close()
    cur_myconn.close()
    myconn.close()
    print getdate(),'rank_id',rank_id,'task_id',task_id,'thread_id',thread_id,'finished and quit\n'
#################################################单独的进程只负责将数据插入到mysql############################################################################################################################################
def insert_mysql_no_getdata(task_id,dst_serverinfo,redis_serverinfo,env,tmplist,mssql_tbname,mysql_tbname,thread_id,sql_input,shard_num):
    dst_server_host=dst_serverinfo[0]
    dst_server_port=dst_serverinfo[1]
    dst_server_db=dst_serverinfo[2]
    redis_host=redis_serverinfo[0]
    redis_port=redis_serverinfo[1]
######创建transfer 服务器数据转移记录连接
    myconn_datatransfer_control_flag, myconn_datatransfer_control = get_database_connections(host='datatransfer-control.mysql.db.ctripcorp.com',port=,db='datatransfer_controldb',user=user_datatransfer_control,password=passwd_datatransfer_control,dbtype='mysql')
    if myconn_datatransfer_control_flag == -1:
        print getdate(), 'task_id', task_id, 'thread_id', thread_id, 'connects datatransfer_control fails'
        sys.exit()
    cur_myconn_datatransfer = myconn_datatransfer_control.cursor()
######创建redis server 连接
    redis_flag, redis_conn = get_redis_conn(redis_host, redis_port)
########创建mysql数据导入目的服务器连接
    myconn_flag,myconn=get_database_connections(host=dst_server_host,port=dst_server_port,db=dst_server_db,user=user_serveradmin,password=passwd_serveradmin,dbtype='mysql')
    if myconn_flag==-1:
        print getdate(),'task_id',task_id,'thread_id',thread_id,'connects server',dst_server_host,'fails\n'
        sys.exit()
######创建transfer 服务器数据转移记录连接
    cur_myconn=myconn.cursor()
    tt=tmplist
    len_tt=len(tt)
    start_time=getdate()
####检测分片数量
    if shard_num==1:     ####不分表
        k=0
        while True:
            sleep_seconds_str = redis_conn.hget('datatransfer_control', 'sleep_seconds')
            if (not sleep_seconds_str) or (sleep_seconds_str == '0'):
                sleep_seconds = 0.1
            else:
                sleep_seconds = float(sleep_seconds_str)
            tmp_inputdata1 = tt[k:k + 500]
            tmp_inputdata2 = tt[k + 500:k + 502]
            if not tmp_inputdata1:
                break
            if len(tmp_inputdata2) == 1:
                tmp_inputdata1 = tt[k:]
                k = k + 1
            try:
                cur_myconn.executemany(sql_input, tmp_inputdata1)
                myconn.commit()
            except Exception, e:
                print '\033[1;31;40m'
                print thread_id,Exception, e
                print '\033[0m'
                sleep(5)
            del tmp_inputdata1
            k += 500
            sleep(sleep_seconds)
        del k
        try:
            cur_myconn_datatransfer.execute(
                "insert into transfer_records(task_id,mssql_tbname,mysql_tbname,thread_id,start_id,end_id,rows,start_time,end_time) values(%s,%s,%s,%s,%s,%s,%s,%s,%s)",(task_id,env+'_'+mssql_tbname,env+'_'+mysql_tbname, thread_id, '-1', '-1', len_tt, start_time, getdate()))
            myconn_datatransfer_control.commit()
        except Exception, e:
            print '\033[1;31;40m'
            print 'myconn_datatransfer', Exception, e
            print '\033[0m'
        myconn.commit()
    else:                                    #分表，重新生产sql_input 语句
        sql1=sql_input[:sql_input.find('(')-1]
        sql2=sql_input[sql_input.find('('):]
        result_classfied = []
        for i in range (shard_num):
            tt_result=[]
            result_classfied.append(tt_result)
            del tt_result
        for item in tt:
            k=item[0]%shard_num
            result_classfied[k].append(item)
        del tt
        for i in range(len(result_classfied)):
            print getdate(),"task_id_"+str(task_id)+" thread_"+str(thread_id)+": now do the data transfer for table "+sql1.upper().replace('INSERT INTO ','')+str(i)
            sql_input=sql1+str(i)+sql2
            k=0
            while True:
                sleep_seconds_str = redis_conn.hget('datatransfer_control', 'sleep_seconds')
                if (not sleep_seconds_str) or (sleep_seconds_str == '0'):
                    sleep_seconds = 0.1
                else:
                    sleep_seconds = float(sleep_seconds_str)
                tmp_inputdata1 = result_classfied[i][k:k + 800]
                tmp_inputdata2 = result_classfied[i][k + 800:k + 802]
                if not tmp_inputdata1:
                    break
                if len(tmp_inputdata2)==1:
                    tmp_inputdata1 = result_classfied[i][k:]
                    k+=1
                try:
                    cur_myconn.executemany(sql_input, tmp_inputdata1)
                    myconn.commit()
                except Exception, e:
                    print '\033[1;31;40m'
                    print thread_id, Exception, e
                    print '\033[0m'
                    sleep(5)
                del tmp_inputdata1
                k += 800
                sleep(sleep_seconds)
            del k
            try:
                cur_myconn_datatransfer.execute(
                    "insert into transfer_records(task_id,mssql_tbname,mysql_tbname,thread_id,start_id,end_id,rows,start_time,end_time) values(%s,%s,%s,%s,%s,%s,%s,%s)",
                    (task_id,env+'_'+mssql_tbname,env+'_'+mysql_tbname.replace("0",str(i)), thread_id, '-1', '-1', len(result_classfied[i]), start_time, getdate()))
                myconn_datatransfer_control.commit()
            except Exception, e:
                print '\033[1;31;40m'
                print 'myconn_datatransfer', Exception, e
                print '\033[0m'
            myconn.commit()
####关闭cursor，关闭连接
    cur_myconn_datatransfer.close()
    myconn_datatransfer_control.close()
    cur_myconn.close()
    myconn.close()

####开启多进程处理
def mutlti_processing_insert(task_id,src_serverinfo,dst_serverinfo,redis_serverinfo,env,rank_id,last_rank,tmplist,mssql_tbname,mysql_tbname,sql_getdata,sql_input,pk_name,shard_num,is_thread_getdata):
    print getdate(),'Enter multi_processing_insert'
    redis_host=redis_serverinfo[0]
    redis_port=redis_serverinfo[1]
    redis_flag, redis_conn = get_redis_conn(redis_host, redis_port)
    redis_conn.hset('datatransfer_control', 'rank_finished', 0)
    p1 = multiprocessing.Process(target=check_replication_delay, args=(task_id, dst_serverinfo, redis_serverinfo))
    p1.start()
    process_list = []
    process_num = 5
    if is_thread_getdata==1:
        end_num = tmplist[1]
        start_num = tmplist[0]
        slice_num = (end_num - start_num) / process_num
        print 'rank_id',rank_id,'call multiprocessing to do the work!'
        sleep(5)
        for i in range(process_num):
            if i == process_num - 1:
                p = multiprocessing.Process(target=insert_mysql,args=(task_id,src_serverinfo,dst_serverinfo,redis_serverinfo,env,rank_id,last_rank,mssql_tbname,mysql_tbname,i,sql_getdata,sql_input,pk_name,shard_num,start_num + i * slice_num,end_num))
            else:
                p = multiprocessing.Process(target=insert_mysql,args=(task_id,src_serverinfo,dst_serverinfo,redis_serverinfo,env,rank_id,0,mssql_tbname,mysql_tbname,i,sql_getdata,sql_input,pk_name,shard_num,start_num + i * slice_num,start_num + (i + 1) * slice_num))
            process_list.append(p)
            p.start()
        for process in process_list:
            process.join()
    else:
        end_num = len(tmplist)
        start_num = 0
        slice_num = (end_num - start_num) / process_num
        for i in range(process_num):
            if i == process_num - 1:
                p = multiprocessing.Process(target=insert_mysql_no_getdata,args=(task_id,dst_serverinfo,redis_serverinfo,env,tmplist[start_num + i * slice_num:],mssql_tbname,mysql_tbname,i,sql_input,shard_num))
            else:
                p = multiprocessing.Process(target=insert_mysql_no_getdata,args=(task_id,dst_serverinfo,redis_serverinfo,env,tmplist[start_num + i * slice_num:start_num + (i + 1) * slice_num],mssql_tbname,mysql_tbname,i,sql_input,shard_num))
            process_list.append(p)
            p.start()
        for process in process_list:
            process.join()
    redis_conn.hset('datatransfer_control', 'rank_finished', 1)
    p1.join()

#######主要针对复合主键情况，若为符合主键则在configdb下生成主键临时表，初始化主键值临时表，改写sql_getdata语句 为join configdb下临时表的类型
def prepare_work(task_id,task_info):
    src_server_host=task_info[0]
    src_server_port = task_info[1]
    src_server_db=task_info[2]
    mssql_table_ret=task_info[3]
    mysql_table_ret=task_info[4]
    sql_getdata_ret=task_info[5]
    sql_insertdata_ret=task_info[6]
    pk_name_ret=task_info[7]
    shard_num_ret=task_info[8]
    src_table_name = mssql_table_ret
    min_pk_value=0
    max_pk_value=0
#####创建 source 服务器连接
    mssql_flag,mssql=get_database_connections(host=src_server_host,port=src_server_port,db=src_server_db,user='',password='',dbtype='sqlserver')
    if mssql_flag==-1:
        print getdate(),'task_id',task_id,'connects ',src_server_host,'failed!'
        sys.exit()
    cur_mssql=mssql.cursor()
    if mssql_table_ret.find('configdb')==-1:
        cur_mssql.execute("exec sp_spaceused "+mssql_table_ret)
        table_rows = cur_mssql.fetchone()
    else:
        mssql_flag_cfg,mssql_cfg=get_database_connections(host=src_server_host,port=src_server_port,db='configdb',user='',password='',dbtype='sqlserver')
        cur_mssql_cfg=mssql_cfg.cursor()
        cur_mssql_cfg.execute("exec sp_spaceused "+mssql_table_ret.replace("configdb..",''))
        table_rows=cur_mssql_cfg.fetchone()
    rows=long(table_rows[1])
#######判断是否为复合主键或主键类型非int，非bigint
    pk_type_flag=1                     #判断主键类型
    if pk_name_ret.find(',')==-1:
        if mssql_table_ret.find('configdb')==-1:
            cur_mssql.execute("SELECT a.name,b.name FROM sys.columns a JOIN sys.types b ON a.system_type_id=b.system_type_id JOIN sys.tables c ON a.object_id=c.object_id WHERE a.name='"+pk_name_ret+"' AND c.name='"+mssql_table_ret+"' AND b.name IN ('smallint','int','bigint','float','real','decimal','numeric')")
        else:
            cur_mssql.execute("SELECT a.name,b.name FROM configdb.sys.columns a JOIN configdb.sys.types b ON a.system_type_id=b.system_type_id JOIN configdb.sys.tables c ON a.object_id=c.object_id WHERE a.name='"+pk_name_ret+"' AND c.name='"+mssql_table_ret.replace('configdb..','')+"' AND b.name IN ('smallint','int','bigint','float','real','decimal','numeric')")
        pk_type=cur_mssql.fetchone()
        if not pk_type:
            pk_type_flag=0
        else:
            cur_mssql.execute("select min("+pk_name_ret+"),max("+pk_name_ret+") from "+mssql_table_ret+"(nolock)")
            pk_min_max_value=cur_mssql.fetchone()
            min_pk_value=pk_min_max_value[0]
            max_pk_value=pk_min_max_value[1]
    if (pk_name_ret.find(',')!=-1) or (pk_type_flag==0):
        tmp_split_columns = pk_name_ret.split(',')
        sql_pk_columns=''
        join_sql=" join configdb..tmp_"+mssql_table_ret+" on "
        for tmp_split_columns_item in tmp_split_columns:
            src_table_name+='_'+tmp_split_columns_item.replace(' ','')
            sql_pk_columns+=tmp_split_columns_item.replace(' ','')+','
            join_sql+=mssql_table_ret+"."+tmp_split_columns_item+"= tmp_"+mssql_table_ret+"."+tmp_split_columns_item+" and "
        sql_pk_columns=sql_pk_columns[:-1]
        join_sql=join_sql[:-5].replace("tmp_"+mssql_table_ret,src_table_name)
        sql_getdata_ret+=join_sql
########创建config主键值临时表,若已存在，则清空该表，若不存在则创建该表，并创建相应的索引
        cur_mssql.execute("select name from configdb.sys.tables where name='"+src_table_name+"'")
        pk_table=cur_mssql.fetchone()
        if  pk_table:
            cur_mssql.execute("TRUNCATE TABLE configdb.."+src_table_name)
            print "truncate table "+src_table_name+"!"
        else:
            print "create a new temporary primary key value table"
            cur_mssql.execute("select "+sql_pk_columns+" into configdb.."+src_table_name+" from "+mssql_table_ret+"(nolock) where 1=0")
            cur_mssql.execute("alter table configdb.."+src_table_name+" add id_new bigint identity(1,1)")
            cur_mssql.execute("create clustered index idx_"+src_table_name+" on configdb.."+src_table_name+"(id_new)")
            cur_mssql.execute("create index idx_"+src_table_name+"11 on configdb.."+src_table_name+"("+sql_pk_columns+")")
#######填充 cofnigdb下临时主键值表，可能耗时较久
        print getdate(),"Loading primary key data into temprary tables!"
        cur_mssql.execute("insert into configdb.."+src_table_name+"("+sql_pk_columns+") select "+sql_pk_columns+" from "+mssql_table_ret+"(nolock)")
        mssql.commit()
        cur_mssql.execute("select min(id_new),max(id_new) from configdb.."+src_table_name+"(nolock)")
        pk_min_max_value=cur_mssql.fetchone()
        min_pk_value=pk_min_max_value[0]
        max_pk_value=pk_min_max_value[1]
        rows=pk_min_max_value[1]
        mssql_table_ret=src_table_name
        pk_name_ret = 'id_new'
#######初始化工作完成，返回更新后的信息，交予main 处理
    return{'src_table_name':src_table_name,'dest_table_name':mysql_table_ret,'min_pk_value':min_pk_value,'max_pk_value':max_pk_value,'sql_getdata':sql_getdata_ret,'sql_insertdata':sql_insertdata_ret,'pk_name':pk_name_ret,'shard_num':shard_num_ret,'rows':rows}

def cleanup_work():
    print 'I am clean up worker\n'

def sub_main():
    current_date=getdate()
    redis_serverinfo=['127.0.0.1',6379]
    ######创建transfer 服务器数据转移记录连接
    myconn_datatransfer_control_flag, myconn_datatransfer_control = get_database_connections(host='',port=,db='', user=user_serveradmin,password=passwd_serveradmin, dbtype='mysql')
    if myconn_datatransfer_control_flag == -1:
        print getdate(), 'Main function: connects datatransfer_control fails'
        sys.exit()
    cur_myconn_datatransfer = myconn_datatransfer_control.cursor(my.cursors.DictCursor)
    env='fat'
    sql_getinformation="select * from sqltoolsdb.tasks_form where run_appname='datatransfer' and status='ready' order by id limit 1"
    cur_myconn_datatransfer.execute(sql_getinformation)
    tt=cur_myconn_datatransfer.fetchone()
    if  tt:
        cur_myconn_datatransfer.execute("update sqltoolsdb.tasks_form set  status='Running' where id=" + str(tt['id']))
        myconn_datatransfer_control.commit()
#        myconn_datatransfer_control.close()
        task_info_tt=eval(tt['args_json'])
        if int(task_info_tt['dest_port'])==55111:
            env='fat'
        elif int(task_info_tt['dest_port'])==55444:
            env='lpt'
        elif int(task_info_tt['dest_port'])==55777:
            env='uat'
        else:
            env='prd'
        task_info=[task_info_tt['src_host'],task_info_tt['src_port'],task_info_tt['src_database'],task_info_tt['mssql_table'],task_info_tt['mysql_table'],task_info_tt['sql_getdata'],task_info_tt['sql_insertdata'],task_info_tt['pk_name'],task_info_tt['shard_num']]
        task_info_new=prepare_work(tt['id'],task_info)
        transfer_result_flag=1
        try:
            if task_info_new['rows']>200000:
                end_num = task_info_new['max_pk_value']
                start_num = task_info_new['min_pk_value']
                rank_num = task_info_new['rows']/200000
                slice_num = (end_num - start_num) /rank_num
                last_rank=0
                try:
                    cur_myconn_datatransfer.execute(
                        "insert into tasks_formlog(formid,operationtype,level,operationdetail,creator)values(%s,%s,%s,%s,%s)",
                        (tt['id'], u'任务开始', 'info', 'Loading data from MSSQL', 'gqhao'))
                    myconn_datatransfer_control.commit()
                except Exception, e:
                    print Exception, e
                for i in range(rank_num):
                    print "################################## start rank: ",i+1,'/',rank_num," ########################"
                    tt_pk1=[]
                    if i == rank_num - 1:
                        tt_pk1.append(start_num + i * slice_num)
                        tt_pk1.append(end_num)
                        last_rank=1
                    else:
                        tt_pk1.append(start_num + i * slice_num)
                        tt_pk1.append(start_num + (i + 1) * slice_num)
                    mutlti_processing_insert(tt['id'],(task_info_tt['src_host'],task_info_tt['src_port'],task_info_tt['src_database']),(task_info_tt['dest_host'],task_info_tt['dest_port'],task_info_tt['dest_database']),redis_serverinfo,env,i,last_rank,tt_pk1,task_info_new['src_table_name'],task_info_new['dest_table_name'],task_info_new['sql_getdata'],task_info_new['sql_insertdata'],task_info_new['pk_name'],task_info_new['shard_num'],1)
                    del tt_pk1
                    finish_percent=format((i+1)*1.0/rank_num*100,'0.4f')
                    try:
                        cur_myconn_datatransfer.execute("insert into tasks_formlog(formid,operationtype,level,operationdetail,creator)values(%s,%s,%s,%s,%s)", (tt['id'], u'任务执行中', 'info',str(finish_percent)+'%', 'gqhao'))
                        myconn_datatransfer_control.commit()
                    except Exception,e:
                        print Exception,e
            else:
                mssql_flag, mssql = get_database_connections(host=task_info_tt['src_host'], port=task_info_tt['src_port'], db=task_info_tt['src_database'],user='', password='',dbtype='sqlserver')
                if mssql_flag == -1:
                    print getdate(), 'connect ',task_info_tt['src_host'], 'failed!'
                    sys.exit()
                cur_mssql = mssql.cursor()
                print getdate(), 'Loading data from source database',task_info_tt['src_host'],task_info_tt['src_database']
                try:
                    cur_myconn_datatransfer.execute("insert into tasks_formlog(formid,operationtype,level,operationdetail,creator)values(%s,%s,%s,%s,%s)",(tt['id'], u'任务开始', 'info','Loading data from source database '+task_info_tt['src_host']+' '+task_info_tt['src_database'], 'gqhao'))
                    myconn_datatransfer_control.commit()
                except Exception, e:
                    print 'insert into tasks_formlog',Exception, e
                cur_mssql.execute(task_info_tt['sql_getdata'])
                tt_data=cur_mssql.fetchall()
                mssql.close()
                try:
                    cur_myconn_datatransfer.execute("insert into tasks_formlog(formid,operationtype,level,operationdetail,creator)values(%s,%s,%s,%s,%s)",(tt['id'], u'任务执行中', 'info','Transfering data from ' + task_info_tt['src_host'] + '.' + task_info_tt['src_database'] + ' to ' + task_info_tt['dest_host'] + '.' + task_info_tt['dest_database'],'gqhao'))
                    myconn_datatransfer_control.commit()
                except Exception, e:
                    print 'status_running insert into tasks_formlog', Exception, e
                mutlti_processing_insert(tt['id'], (task_info_tt['src_host'], task_info_tt['src_port'], task_info_tt['src_database']),(task_info_tt['dest_host'], task_info_tt['dest_port'], task_info_tt['dest_database']),redis_serverinfo,env, 0, 0, tt_data,task_info_new['src_table_name'], task_info_new['dest_table_name'],task_info_new['sql_getdata'], task_info_new['sql_insertdata'],task_info_new['pk_name'], task_info_new['shard_num'], 0)
        except Exception,e:
            print Exception,e
            transfer_result_flag=0
        myconn_datatransfer_control_flag, myconn_datatransfer_control = get_database_connections(host='', port=, db='', user=user_serveradmin,password=passwd_serveradmin, dbtype='mysql')
        if myconn_datatransfer_control_flag == -1:
            print getdate(), 'Main function: connects datatransfer_control fails'
            sys.exit()
        cur_myconn_datatransfer = myconn_datatransfer_control.cursor(my.cursors.DictCursor)
        if transfer_result_flag==0:
            cur_myconn_datatransfer.execute("update sqltoolsdb.tasks_form  set  status='Failed' where id=" + str(tt['id']))
            myconn_datatransfer_control.commit()
            print 'Something wrong happended,please check!'
        else:
            cur_myconn_datatransfer.execute("update sqltoolsdb.tasks_form set  status='Finished' where id=" + str(tt['id']))
            myconn_datatransfer_control.commit()
            try:
                cur_myconn_datatransfer.execute("insert into tasks_formlog(formid,operationtype,level,operationdetail,creator)values(%s,%s,%s,%s,%s)", (tt['id'], u'任务完成', 'info','迁移完成', 'gqhao'))
                myconn_datatransfer_control.commit()
            except Exception, e:
                print Exception, e
        myconn_datatransfer_control.close()
    else:
        print current_date,'I am alive,but no work to do!\n'
def main():
    while True:
        try:
            sub_main()
        except Exception,e:
            print Exception,e
        sleep(5)
if __name__=='__main__':
    main()
