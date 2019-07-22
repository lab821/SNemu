#!/usr/bin/env python3
import time
import matplotlib.pyplot as plt
import pandas as pd
import requests

lsid = [0]
sdict = {}

def tranfer2dict(logline,starttime):
    row = {}
    parts = logline.split()
    row['src'] = 3 - int(parts[0][-1])
    row['time'] = parts[1]
    row['rtime'] = timestamp(parts[1]) - timestamp(starttime)
    row['dst'] = int(parts[2].split('.')[-1].split(':')[0]) - 134
    row['size'] = int(parts[3].split('=')[-1])
    fid = int(parts[4])
    # print(fid)
    if fid in sdict.keys():
        # print(self.sdict.keys())
        row['sid'] = sdict[fid]
    else:
        lsid[0] += 1
        sdict[fid] = lsid[0]
        row['sid'] = sdict[fid]
    row['mid'] = int(parts[5])
    return row

class transfer:
    lsid = [0]
    sdict = {}
    def __init__(self, logline, starttime):
        parts = logline.split()
        self.src = 3 - int(parts[0][-1])
        self.time = parts[1]
        self.rtime = timestamp(self.time) - timestamp(starttime)
        self.dst = int(parts[2].split('.')[-1].split(':')[0]) - 134
        self.size = int(parts[3].split('=')[-1])
        fid = int(parts[4])
        # print(fid)
        if fid in self.sdict.keys():
            # print(self.sdict.keys())
            self.sid = self.sdict[fid]
        else:
            self.lsid[0] += 1
            self.sdict[fid] = self.lsid[0]
            self.sid = self.sdict[fid]
        self.mid = int(parts[5])

def timestamp(timestring):
    date_time = timestring.split(',')[0]
    ms_time = timestring.split(',')[1]
    structedtime = time.strptime(date_time, "%y/%m/%d/%H:%M:%S")
    # print(structedtime)
    time_stamp = int(time.mktime(structedtime) * 1000 + int(ms_time))
    return time_stamp

def logresolve(dirpath):
    data = pd.DataFrame(columns=['src', 'rtime','time', 'dst', 'size', 'sid', 'mid'])
    executorlogpath = dirpath + 'executor_0'
    with open(executorlogpath,'r') as exelog:
        timeline = exelog.readline()
        while timeline:
            if 'Started daemon with process name' in timeline:
                starttime = timeline.split()[1]
                # print(starttime)
                break
            timeline = exelog.readline()
    logpath = dirpath + 'logs.txt'
    transferlist = []
    with open(logpath,'r') as log:
        rawtransfer = log.readline()
        while rawtransfer:
            row = tranfer2dict(rawtransfer,starttime)
            # print(row)
            data = data.append([row], ignore_index=True)
            # print(data)
            rawtransfer = log.readline()
    return data

class coflow:
    def __init__(self):
        self.rtime =1000000
        self.mid = []
        self.dst = 0
        self.fdict = {}

if __name__ == '__main__':
    dirname = '/mnt/nas/spark_executor_logs_and_traces/ScalaPageRank/3/'
    appname = input('input appId:')
    if not appname:
        appname = requests.get("http://net4:18080/api/v1/applications").json()[0]['id']
    dirname = dirname + appname +'/'
    tracename = input('input trace file name:')
    if not tracename:
        tracename = 'trace.txt'
    # dirname = ''
    # print(dirname)
    # data = pd.read_csv(dirname+'logs.txt',sep=' ',header=None)
    # data.loc[data[data[0]=='executor_0'].index,0]=3
    # temp =
    data = logresolve(dirname)
    # print(data)
    coflows = []
    for sid in range(1,6):
        for dst in range(1,4):
            qdata = data.query('sid ==' + str(sid) + '& dst ==' + str(dst))
            while qdata.size > 0:
                cf = coflow()
                cf.dst = dst
                for index, row in qdata.iterrows():
                    if row['mid'] not in cf.mid:
                        cf.mid.append(row['mid'])
                        if row['src'] not in cf.fdict.keys():
                            cf.fdict[row['src']] = row['size']
                        else:
                            cf.fdict[row['src']] += row['size']
                        if row['rtime'] < cf.rtime:
                            cf.rtime = row['rtime']
                        qdata = qdata.drop(index)
                #print(cf.rtime)
                #print(cf.mid)
                #print(cf.dst)
                #print(cf.fdict)
                coflows.append(cf)
    with open(tracename, 'w') as f:
        f.write('3 ' + str(len(coflows)) + '\n')
        index = 1
        for cf in coflows:
            line = ' '.join([str(index), str(cf.rtime), str(2)])
            for src in cf.fdict.keys():
                line = line + ' ' + str(src)
            line = ' '.join([line, str(1), str(cf.dst)])
            for src in cf.fdict.keys():
                line = line + ' ' + str(round((cf.fdict[src]/ 1048576), 2))
            f.write(line + '\n')
            index += 1
            # print(qdata)
            # break
    # print(data)
    # print(data[data.iloc[:,0]=='executor_0'])
    # print(data.keys())
    # for i in data:
    #     print(i.sid)
    # nodes = [[],[],[]]
    # for node in range(1,4):

