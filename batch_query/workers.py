#!/usr/bin/python
import time,datetime
import threading
import os


#----------------------------------------------------
def init_servers():
    server0=Server("220.164.140.233")
    server1=Server("220.164.140.235")
    server2=Server("60.161.139.18")
    server3=Server("60.161.139.19")
    server4=Server("121.10.237.169")
    server5=Server("121.10.239.5")
    server6=Server("61.166.10.100")
    SERVERS=[server0,server1,server2,server3,server4,server5,server6]
    return SERVERS
#----------------------------------------------------
def distr_tasks(taskfile,SERVERS):
    with open(taskfile,'r') as data:
     server_num=len(SERVERS)
     cqls=[]
     for each in data:
       if each.startswith("select"):
          each = each[0:-1]
          cqls.append(each)
    total_task_num=len(cqls)
    each_server_number=int(total_task_num/server_num)
    mod=total_task_num%server_num
    for n in range(0,server_num):
        if mod > 0:
            number=each_server_number+1
            mod=mod-1
        else:
            number=each_server_number
        for i in range(n*number,(n+1)*number):
            SERVERS[n].add_cql(cqls[i])
#====================================================
class Server:
    def __init__(self,ip):
        self.ip=ip
        self.cqls=[]
    def add_cql(self,cql):
        self.cqls.append(cql)
    def merge_result(self):
        log=set()
        err=set()
        logfiles=os.listdir('.')
        for filename in logfiles:
            
            if filename.endswith('.log'):
               print(filename)
               with open(filename,'r')as file:
                  for sentence in file:
                    #print (sentence)
                    log.add(sentence)
               with open('logfile','a+') as logfile:
                  for each in log:
                    #print (each)
                    logfile.write(each)
                    #print (each,file=logfile)
            if filename.endswith('.err'):
               print(filename)
               with open(filename,'r')as file:
                  for sentence in file:
                    #print (sentence)
                    err.add(sentence)
               with open('errfile','a+') as errfile:
                  for each in err:
                    #print (each)     
                    errfile.write(each)
                    #print (each,file=errfile)

    def work(self,i,counter):
        number=0
        strnum=''
        for str in self.cqls:
            str1="echo 'use vcmis;"+str+"'|/cassandra/bin/cqlsh -2 "+self.ip
            print('...........................')
            print(str1)
            result=os.popen(str1)
            ifok='no'
            for line in result:
                line=line.strip().lstrip()
                if line.find('timeout')!=-1:
                    ifok='no'
                    
                if line.find('count')!=-1:
                    ifok='ok'
                if line.isdigit():
                    print(line)
                    strnum=line
                    number=int(line)
                #print(ifok)
                if ifok!='ok':
                   with open(self.ip+'.err','a+') as errfile:
                      print >>errfile,str
                   continue
                else:
                   with open(self.ip+'.log','a+') as logfile:
                        logstr=str+strnum
                        #logfile.write(logstr)
                        print>>logfile,logstr
                g_mutex.acquire()
                counter.inc()
                g_mutex.release()


#====================================================
class Counter:
    def __init__(self):
        self.total=0
    def inc(self,step=1):
        self.total=self.total+step
    
##########################################
#               main                      #
#                                         #
###########################################
if __name__ == "__main__":
   os.system('rm *.log')
   os.system('rm *.err') 
   global g_mutex
   counter=Counter();
   i=0
   startTime = datetime.datetime.now()
   print ("start time: " , startTime)
   SERVERS=init_servers()
   distr_tasks('cqls.txt',SERVERS)
   server_number=len(SERVERS)
   #global g_mutex
   #init thread pool
   thread_pool = []
   #init mutex
   g_mutex = threading.Lock()
   # init thread items
   for server in SERVERS:
       th = threading.Thread(target=server.work,args=(i,counter))
       thread_pool.append(th)
   '''for i in range(server_number):
       th = threading.Thread(target=worker,args=(i,acc) ) ;
       thread_pool.append(th)
    '''
   # start threads one by one
   for i in range(server_number):
       thread_pool[i].start()
   #collect all threads
   for i in range(server_number):
       threading.Thread.join(thread_pool[i])

   server.merge_result()
   endTime = datetime.datetime.now()
   print ("total",counter.total,"cql was exec")
   print ("program ending at " ,endTime , " spend time " , endTime-startTime);
