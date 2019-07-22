#!/usr/bin/python

import random as ra

def random_list(count,node_count):
    serial_list = []
    serial_list=ra.sample(range(1,node_count+1),count)
    return serial_list

def Trace(x=2, y=2):
    node_count = x
    coflow_count = y
    file_name = 'Trace.txt'
    f = open(file_name,'w+')
    s = ''
    s = s+str(node_count)+' '+str(coflow_count)+'\n'
    f.write(s)
    for i in range(1,coflow_count+1):
        s=''
        #coflow tag
        s=s+str(i)+' '
        #start time
        s=s+str(ra.randint(0,1000))+' '
        #mapper count
        m = ra.randint(1,node_count)
        s=s+str(m)+' '
        #mapper list
        mapper_list = random_list(m,node_count)
        mapper_list.sort()
        for j in mapper_list:
            s=s+str(j)+' '
        #reducer count
        r = ra.randint(1,node_count)
        s=s+str(r)+' '
        #reducer list
        reducer_list = random_list(r,node_count)
        reducer_list.sort()
        for j in reducer_list:
            s=s+str(j)+' '
        #transport data count
        # print(i,end=' ')
        # print(ra.randint(0,10000),end=' ')
        data_list = []
        for a in mapper_list:
            for b in reducer_list:
                #debug
                #print("(%d,%d)"%(a,b),end = ':')
                if a != b:
                    s=s+str(ra.randint(10,1000))+' '
                else:
                    s=s+str(0)+' '
        s=s+'\n'
        f.write(s)
    f.close()

if __name__=='__main__':
    Trace()
