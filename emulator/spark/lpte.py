#!/usr/bin/env python3


#step1:copy logas
#step2:parse logs
#step3:extract trace
import os
import re
import requests
import TraceExtractor as trace
import metricsExtractor as metrics

def logparser (path,filename):
    logfile = open(path+ '/' + filename, 'r')
    outfile = open(path+ '/' + 'logs.txt', 'a')
    line = logfile.readline()
    while line:
        usefulline = re.match(r'.*?TransportRequestHandler: Sent result ChunkFetchSuccess.*?shuffle_.*', line)
        if usefulline is not None:
            #debug
            #print('line matched: ' + line)
            parts = usefulline.group().split(' ')
            outfile.write(filename + ' ' + parts[1] + ' ' + parts[-1].split('/')[1] + ' ' + parts[-4].split('}')[0]  + ' ' + parts[-6].split('/')[-1].split('_')[1] + ' ' + parts[-6].split('/')[-1].split('_')[2] + ' '+ parts[-6].split('/')[-1].split('_')[-1].split('.')[0] + '\n')
        line = logfile.readline()
    outfile.close()
    logfile.close()

if __name__ == '__main__':
    # request spark rest api
    appInfo = requests.get("http://net4:18080/api/v1/applications").json()[0]
    appId = appInfo['id']
    appName = appInfo['name']
    executorsInfo = requests.get("http://net4:18080/api/v1/applications/" + appId + "/allexecutors").json()

    # exclude dead executors
    executorList = []
    for executor in executorsInfo:
        if executor['isActive'] and executor['id'] != 'driver':
            executorList.append(executor['id'])
    # executorCount = len(executorsInfo) - 1 # despite a driver node
    executorCount = len(executorList)

    # copy logs
    if 'KMeans' in appName:
        appName = 'ScalaKMeans'
    print("Getting logs of " + appName)
    logSavePath = '/mnt/nas/spark_executor_logs_and_traces/' + appName + '/' + str(executorCount) + '/' +appId
    if not os.path.exists(logSavePath):
        os.makedirs(logSavePath)
    for executor in executorsInfo:
        if executor['id'] != 'driver' and executor['isActive']:
            executorHost = executor['hostPort'].split(':')[0]
            remoteUserName = 'pc'
            os.system('scp %s@%s:$SPARK_HOME/work/%s/%s/stderr %s/executor_%s' % (remoteUserName, executorHost, appId, executor['id'], logSavePath, executor['id']))
            os.system('scp %s@%s:$SPARK_HOME/work/%s/%s/stderr %s/exelog_%s' % (remoteUserName, executorHost, appId, executor['id'], logSavePath, executorHost))
            os.system('scp %s@%s:/home/%s/logs/executorgc.log %s/gclog_%s.log' % (remoteUserName, executorHost, remoteUserName, logSavePath, executor['id']))
    shortAppName = appName[5:].lower()
    os.system('cp $HIBENCH_HOME/report/%s/spark/bench.log %s/driverlog' % (shortAppName, logSavePath))

    #parse log
    fileList = os.listdir(logSavePath)
    if 'logs.txt' in fileList:
        print ('Already parsered.')
    else:
        print ('Parsering.....')
        for item in fileList:
            if 'executor' in item:
                logparser(logSavePath,item)

    print ('Traces Extracting')
    #extract trace
    logfile = logSavePath + '/' + 'logs.txt'
    matrixs,TIME_LIST = trace.LogtoMatrixs(logfile)
    tracefile = logSavePath + '/' +'trace.txt'
    trace.MatrixsToTrace(tracefile,matrixs,TIME_LIST)
    print ('Extracting success')
    #extract metrics
    metricsfile = logSavePath + '/' + 'metrics.json'
    metrics.metricsCollector(0,metricsfile)
    print ('All Done')
