#!/usr/bin/python
import os
import sys
if len(sys.argv)<2:
    print('you should input:',sys.argv[0],'cql')
    exit()
if len(sys.argv)==2:
   cql=sys.argv[1]
   command1='echo "use vcmis;'+cql+'"|/cassandra/bin/cqlsh 220.164.140.233'
   print (command1)
   os.system(command1)
if len(sys.argv)==3:
   ip=sys.argv[1]
   cql=sys.argv[2]
   command1='echo "use vcmis;'+cql+'"|/cassandra/bin/cqlsh '+ip
   print (command1)
   os.system(command1)



