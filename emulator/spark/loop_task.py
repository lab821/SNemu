#!/usr/bin/env python3
import os
import time
import argparse
import re
import shutil


def set_parallelism(mapside, reduceside, dataset):
    filedata = ""
    conf_path = "/home/pc/hibench/HiBench/conf/hibench.conf"
    with open(conf_path, 'r') as hibenchconf:
        for line in hibenchconf:
            if "hibench.scale.profile" in line:
                line = "hibench.scale.profile " + dataset + "\n"
            elif "hibench.default.map.parallelism" in line:
                line = "hibench.default.map.parallelism " + mapside + "\n"
            elif "hibench.default.shuffle.parallelism" in line:
                line = "hibench.default.shuffle.parallelism " + reduceside + "\n"
            filedata += line
    with open(conf_path, 'w') as hibenchconf:
        hibenchconf.write(filedata)


def set_spark_confs(conflist):
    filedata = ""
    conf_path = "/home/pc/hibench/HiBench/conf/spark.conf"
    conf_dict = {}
    for conf in conflist:
        conf_pair = conf.split('=')
        conf_dict[conf_pair[0]] = conf_pair[1]
    with open(conf_path, 'r') as hibenchconf:
        for line in hibenchconf:
            for conf_name in list(conf_dict.keys())[:]:
                if conf_name in line:
                    line = conf_name + " " + conf_dict[conf_name] + "\n"
                    del conf_dict[conf_name]
            filedata += line
    if conf_dict:
        for conf in conf_dict.keys():
            line = conf + " " + conf_dict[conf] + "\n"
            filedata += line
    with open(conf_path, 'w') as hibenchconf:
        hibenchconf.write(filedata)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--loopcount",
                        help="task loop count",
                        default="1")
    parser.add_argument("-t", "--taskname",
                        help="which task to loop(like micro/wordcount)")
    parser.add_argument("-d", "--dataset",
                        help="which kind of dataset to use(like small,large,huge)",
                        default="large",
                        choices=["tiny", "small", "large", "huge", "gigantic", "bigdata"])
    parser.add_argument("-m", "--map",
                        help="map side parallelism",
                        type=str,
                        default='36')
    parser.add_argument("-r", "--reduce",
                        help="reduce side parallelism",
                        type=str,
                        default='36')
    parser.add_argument("-p", "--prepare",
                        help="input data prepare mode(none, once, every)",
                        default="none",
                        choices=["none", "once", "every"])
    parser.add_argument("-c", "--conf",
                        help="additional spark configs separated by space(eg. spark.driver.memory=4g spark.executor.memory=4g)",
                        nargs='*')

    args = parser.parse_args()
    loop_time = args.loopcount
    task_name = args.taskname
    dataset = args.dataset
    mapside = args.map
    reduceside = args.reduce
    reprepare = args.prepare
    conflist = args.conf

    set_parallelism(mapside, reduceside, dataset)
    # copy default spark conf file
    shutil.copyfile("/home/pc/hibench/HiBench/conf/spark.conf","/home/pc/hibench/HiBench/conf/spark.conf.tmp")
    if conflist:
        set_spark_confs(conflist)

    cmdprepare = '$HIBENCH_HOME/bin/workloads/'+ task_name.lower() + '/prepare/prepare.sh'
    if reprepare == 'once':
        os.system(cmdprepare)
        cmdprepare = ''
    elif reprepare == 'none':
        cmdprepare = ''
    elif reprepare == 'every':
        pass
    cmdrun = '$HIBENCH_HOME/bin/workloads/' + task_name.lower() + '/spark/run.sh'
    for i in range(int(loop_time)):
        if os.system(cmdprepare) == 0:
            if os.system(cmdrun) == 0:
                time.sleep(5)
                os.system('./lpte.py')
    # take back default spark conf
    shutil.copyfile("/home/pc/hibench/HiBench/conf/spark.conf.tmp","/home/pc/hibench/HiBench/conf/spark.conf")



