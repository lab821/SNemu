import os
import re
import requests
import json
import time
from datetime import datetime

def mstimestamp(timestr):
    datetime_obj = datetime.strptime(timestr, "%Y-%m-%dT%H:%M:%S.%f%Z")
    obj_stamp = int(time.mktime(datetime_obj.timetuple()) * 1000.0 + datetime_obj.microsecond / 1000.0)
    return obj_stamp


def metricsCollector(logIndex,metrics_file):

    print("Extract metrics...")
    metrics = dict()
    #get application infomation
    appInfo = requests.get("http://10.134.147.138:18080/api/v1/applications").json()[logIndex]

    #application id
    appId = appInfo['id']
    metrics['appId'] = appId
    #application name
    appName = appInfo['name'] 
    metrics['appName'] = appName
    #application duration
    #from attempts (it is a list,so maybe not the first element if error happened) 
    #unit: ms
    appDuration = appInfo['attempts'][0]['duration']
    metrics['appDuration'] = appDuration
    #get jobs infomation
    #jobsInfo is a list, every element is a job  
    jobsInfo = requests.get("http://10.134.147.138:18080/api/v1/applications/" + appId + "/jobs").json()    
    #debug
    #print("jobsInfo type : " + str(type(jobsInfo)))

    #get the count of jobs
    jobsCount = len(jobsInfo)
    metrics['jobsCount'] = jobsCount
    metrics['jobs'] = list()
    #traverse all the jobs
    #get the jobId of every job
    #get the jobDuration
    #get the stages in every job
    for job in jobsInfo:
        jobInfo = dict()
        #get the jobId
        jobId = job['jobId']
        jobInfo['jobId'] = jobId
        #get the jobDuration
        submissionTime = job['submissionTime']
        completionTime = job['completionTime']
        startTime = mstimestamp(submissionTime)
        endTime = mstimestamp(completionTime)
        jobDuration = endTime - startTime
        jobInfo['jobDuration'] = jobDuration
        #get the satges and count
        stageIds = job['stageIds']
        stageCount = len(stageIds)
        jobInfo['stageCount'] = stageCount
        jobInfo['stageIds'] = stageIds 
        metrics['jobs'].append(jobInfo)

    #get executors Info
    executorsInfo = requests.get("http://10.134.147.138:18080/api/v1/applications/" + appId + "/executors").json()
    #debug
    #print ("executorInfo type : " + str(type(executorsInfo)))

    #traverse all the executors
    #get the executorId of every executor
    #get the count of total tasks of every executor
    executorCount = 0
    metrics['executorsCount'] = executorCount
    metrics['executors'] = list()
    for executor in executorsInfo:
        executorInfo = dict()
        #get the executorId
        executorId = executor['id']
        if executorId == 'driver':
            continue
        #update the executorCount
        executorCount += 1
        #get the completedTasks
        executorTasks = executor['completedTasks']
        executorInfo['executorId'] = executorId
        executorInfo['executorTasks'] = executorTasks
        metrics['executors'].append(executorInfo)
    metrics['executorsCount'] = executorCount

    #get stages information
    stagesInfo = requests.get("http://10.134.147.138:18080/api/v1/applications/" + appId + "/stages").json()
    #debug
    #print ("stagesInfo type : " + str(type(stagesInfo)))
    #get the total stages count
    stagesConut = len(stagesInfo)
    metrics['stagesCount'] = stagesConut
    metrics['stages'] = list()
    #traverse all the stages
    #get the stageId of every stage
    #get the duration of every stage
    #get the mapInput of every stage
    #get the mapOutput of every stage
    #get the reduceInput of every stage
    #get the reduceOutput of every stage
    #get the spill size fo every stage
    #get the remote date size of every stage
    #get the local date size of every stage
    for stage in stagesInfo:
        stageInfo = dict()
        #get stageId
        stageId = str(stage['stageId'])   
        stageInfo['stageId'] = stageId
        #get stage duration
        submissionTime = stage['submissionTime']
        completionTime = stage['completionTime']
        startTime = mstimestamp(submissionTime)
        endTime = mstimestamp(completionTime)
        stageDuration = endTime - startTime       
        stageInfo['stageDuration'] = stageDuration
        #get the mapInput
        mapInput = stage['inputBytes']
        stageInfo['mapInput'] = mapInput
        #get the mapOutput
        mapOutput = stage['shuffleWriteBytes']
        stageInfo['mapOutput'] = mapOutput
        #get the reduceInput
        reduceInput = stage['shuffleReadBytes']
        reduceInput += mapInput
        stageInfo['reduceInput'] = reduceInput
        #get the reduceOutput
        reduceOutput = stage['outputBytes']
        stageInfo['reduceOutput'] = reduceOutput
        #get spill size
        spillSize = stage['memoryBytesSpilled'] + stage['diskBytesSpilled']
        stageInfo['spillSize'] = spillSize
        #get detail of everystage
        detailsInfo = requests.get("http://10.134.147.138:18080/api/v1/applications/" + appId + "/stages/" + stageId).json()
        tasksInfo = detailsInfo[0]['tasks']
        # init tasksCount remoteRead localRead runtime
        tasksCount = 0
        remoteRead = 0
        localRead = 0
        totalrunTime = 0
        for value in tasksInfo.values():
            
            tasksCount += 1
            #get the remote read size through add up of the task in this stage
            remoteRead += value['taskMetrics']['shuffleReadMetrics']['remoteBytesRead']
            localRead += value['taskMetrics']['shuffleReadMetrics']['localBytesRead']
            #get the runTime
            totalrunTime += value['taskMetrics']['executorRunTime']
        stageInfo['tasksCount'] = tasksCount
        stageInfo['remoteRead'] = remoteRead
        stageInfo['localRead'] = localRead
        stageInfo['averagerunTime'] = totalrunTime/tasksCount
        metrics['stages'].append(stageInfo)

    #convert metrics to json format
    #metrics_json = json.dumps(metrics , indent = 4)
    #print(metrics_json)
    f = open(metrics_file,'w+')
    json.dump(metrics , f , indent = 4)
    f.close()
    print("Done...")

if __name__ == "__main__":
    metrics_file = 'metrics.json'
    metricsCollector(0,metrics_file)