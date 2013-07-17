#!/usr/bin/python
import os
import sys
if len(sys.argv)!=2:
    print('you should input:',sys.argv[0],'cql')
    exit()
cql=sys.argv[1]
command1='echo "use vcmis;'+cql+'"|/cassandra/bin/cqlsh 220.164.140.233'
command2='echo "use vcmis;'+cql+'"|/cassandra/bin/cqlsh 61.166.10.100'
command3='echo "use vcmis;'+cql+'"|/cassandra/bin/cqlsh 220.164.140.235'
command4='echo "use vcmis;'+cql+'"|/cassandra/bin/cqlsh 60.161.139.18'
command5='echo "use vcmis;'+cql+'"|/cassandra/bin/cqlsh 60.161.139.19'
command6='echo "use vcmis;'+cql+'"|/cassandra/bin/cqlsh 121.10.237.169'
command7='echo "use vcmis;'+cql+'"|/cassandra/bin/cqlsh 121.10.239.5'

#print (sys.argv[0])
print (command1)
os.system(command1)

print (command2)
os.system(command2)

print (command3)
os.system(command3)

print (command4)
os.system(command4)

print (command5)
os.system(command5)

print (command6)
os.system(command6)

print (command7)
os.system(command7)


