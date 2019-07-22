#从处理后的日志中提取数据流矩阵
import random as ra
import time

def LogtoMatrixs(logfile):
    matrixs = dict()        #数据流矩阵是一个字典，关键字是阶段的列表，内容是一个二维矩阵存储这个阶段的流量传输信息
    stagelist =  list()     #日志中阶段id列表
    executorlist = list()   #日志中节点列表
    iplist = list()         #日志中ip列表
    temp_dict = dict()      #存储executor节点的通信ip列表
    IP_MAP = dict()         #记录每个executor节点的IP地址
    START_TIME = dict()     #记录每个stage开始的时间（第一条流的发起时间）
    #文件目录改
    logfile = open(logfile , 'r')
    #第一遍遍历
    #主要目的是为了自动识别日志中的所有executor节点，stage个数以及各个executor节点的ip
    log = logfile.readline()
    while log:
        parts = log.split(' ')
        #提取生成这条日志的executor_id
        executor_id = parts[0]
        #提取与该executor节点通信的节点的ip
        ip = parts[2].split(':')[0]
        #提取这条日志中的stage_id
        stage_id = parts[4]
        #提取这条日志的时间戳
        stage_time = '20' + parts[1]
        date_time = stage_time.split(',')[0]
        ms_time = stage_time.split(',')[1]
        time_stamp = int(time.mktime(time.strptime(date_time,"%Y/%m/%d/%H:%M:%S"))*1000 + int(ms_time))
        #字典中是否已经存在该节点
        if executor_id not in temp_dict.keys():
            #不存在的话创建该节点的列表
            temp_dict[executor_id]=list()
        else:
            #存在的话这个ip的节点是否已经记录在该executor节点的通讯列表里
            if ip not in temp_dict[executor_id]:
                #如果不在记录这个节点
                temp_dict[executor_id].append(ip)
        #该executor节点是否已经被记录在已知节点列表里
        if executor_id not in executorlist:
            #不在的话加入
            executorlist.append(executor_id)
        #判断这个ip是否已经被记录在ip列表里
        if ip not in iplist:
            #不在的话加入
            iplist.append(ip)
        #判断这个stage_id是否已经被记录
        if stage_id not in stagelist:
            #不在的话加入
            stagelist.append(stage_id)
            #同时记录这条日志的时间戳作为这个阶段的开始时间
            START_TIME[stage_id] = time_stamp
            #debug
            #print(time_stamp)
        #否则检测是否有更早的时间记录（主要用于比较节点间）
        else:
            if START_TIME[stage_id] > time_stamp:
                START_TIME[stage_id] = time_stamp
                #debug
                #print(time_stamp)
        log = logfile.readline()
    #匹配每个节点与其对应ip
    #方法是查找ip列表中有的但是不在executor节点的通信列表中的ip地址
    for executor_id in temp_dict.keys():
        executor_ip_list = [x for x in iplist if x not in temp_dict[executor_id]]
        if len(executor_ip_list):
            IP_MAP[executor_ip_list[0]]=executor_id
        #print (IP_MAP)
  # print('over')
    executorlist.sort()
    iplist.sort()

    #第二遍遍历
    logfile.seek(0)
    log = logfile.readline()
    #节点数量
    s_num = len(executorlist)

    while log:
        parts = log.split(' ')
        #源节点
        executor_id = parts[0]
        #目的节点IP
        ip = parts[2].split(':')[0]
        #数据长度
        length = float(parts[3].split('}')[0].split('=')[1])/(1024*1024)
        #stage编号
        stage_id = parts[4]
        #这里才是stage的顺序，第几个shuffle阶段，等价于第几组coflow
        stage = stagelist.index(stage_id)
        #源节点
        src_e = executorlist.index(executor_id)
        #目的节点
        dst_e = executorlist.index(IP_MAP[ip])
        #创建一个新的shuffle阶段流量矩阵
        if stage not in matrixs.keys():
            matrixs[stage]=[[0]*s_num for row in range(s_num)]
        #一条流的流量大小累加
        matrixs[stage][src_e][dst_e]+=length
        log = logfile.readline()
    logfile.close()
    TIME_LIST = list(START_TIME.values())
    TIME_LIST = [x-TIME_LIST[0] for x in TIME_LIST]
    return matrixs,TIME_LIST

def MatrixsToTrace(Trace_file,matrixs,TIME_LIST):

    f = open(Trace_file,'w+')
    s = str(len(matrixs[0])) + ' ' + str(len(matrixs)) + '\n'
    f.write(s)
    #debug
    #print(s)
    coflow_id = 0
    for stage in matrixs.keys():
        coflow_id += 1
        #coflow tag
        s = str(coflow_id)
        #start time
        if TIME_LIST[coflow_id-1] < 0 :
            TIME_LIST[coflow_id-1]+=3600000
        s = s + ' ' +str(TIME_LIST[coflow_id-1])
        #mapper count
        m = len(matrixs[0])
        s=s + ' ' + str(m)
        #mapper list
        for j in range(1,m+1):
            s=s + ' ' + str(j)
        #reducer count
        r = len(matrixs[0])
        s=s + ' ' + str(m)
        #reducer list
        for j in range(1,r+1):
            s=s + ' ' + str(j)
        #each flow size
        for i in range(len(matrixs[0])):
            for j in range(len(matrixs[0][0])):
                size = round(matrixs[stage][i][j],2)
                s = s + ' ' + str(size)
        s=s+'\n'
        f.write(s)
        #debug
        #print(s)
    f.close()

#打印流量矩阵
def printmatrixsinshell(matrixs):
    for stage in matrixs.keys():
        print('Shuffle_',stage)
        for i in range(len(matrixs[0])):
            for j in range(len(matrixs[0][0])):
                print('%.2f'%matrixs[stage][i][j],end=' ')
            print('')
        print('\n')

#时间戳转换为毫秒
def StrToTime(str_time):
    hour = str_time.split(':')[0]
    minite = str_time.split(':')[1]
    second = str_time.split(':')[2].split(',')[0]
    micosecond =  str_time.split(',')[1]
    timestamp = (3600*int(hour) + 60*int(minite) + int(second))*1000 + int(micosecond)
    return timestamp

if __name__ == '__main__':
    #格式化的日志文件
    logfile = r'E:\日志\logs.txt'
    #Trace文件路径
    Trace_file = 'NewTrace.txt'
    #提取流量矩阵和时间戳序列
    matrixs,START_TIME= LogtoMatrixs(logfile)
    #输出到Trace文件
    MatrixsToTrace(Trace_file,matrixs,START_TIME)

