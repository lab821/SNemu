from mininet.topo import Topo
from mininet.node import (Controller, RemoteController, OVSKernelSwitch,
                          CPULimitedHost, OVSController)
from mininet.net import Mininet
from mininet.link import TCLink
from mininet.cli import CLI
from mininet.util import custom, irange
from mininet.log import setLogLevel, info, warn, error, debug

#from subprocess import Popen, PIPE, call
import subprocess
from argparse import ArgumentParser
import multiprocessing
from time import sleep
import os
import sys
import json

from network import (CoflowEmuNet, DockerSparkNet)
from trace import Trace

#from nat import connectToGateway, establishRoutes, startNAT, stopNAT
#from sshd import startSSH, stopSSH

def RunTest(net=None, remoteController=False, enableNAT=False, trace="/home/pc/mremu/Trace.txt"):

	net.start()
#	info( '*** Starting controller\n' )
#	for controller in net.controllers:
#		controller.start()
#	info( '*** Starting %s switches\n' % len( net.switches ) )
#	for switch in net.switches:
#		info( switch.name + ' ')
#		if switch.name != "mgnt0":
#			switch.start( [net.controllers[0]])

	# wait for the switches to connect to the controller
	info('** Waiting for switches to connect to the controller\n')
	sleep(1)

	hosts = net.hosts
	# start communication between master and slaves
	for n in irange(0, len(hosts)-1):
		host = hosts[n]
		if(n==0):
			# copy the Trace.txt to the master 
			cmd = "docker cp " + trace + " mn.master:/varys/trace.txt"
			#info(cmd)
			returnCode = os.system(cmd)
			info('** Trace file copied to master\n')
			host.popen("nohup ./run coflowemu.framework.master.EmulationMaster --ip 10.0.0.254 --port 1606 --trace /varys/trace.txt")
			sleep(1)
		else:
			slavecmd1 = "nohup ./run coflowemu.framework.slave.Slave coflowemu://10.0.0.254:1606"
			host.popen(slavecmd1)

	sleep(1)
	# start transmission 
	for n in irange(1,len(hosts)-1):
		host = hosts[n]
		# copy Trace.txt to the slaves
		cmd = "docker cp " + trace + " mn.m"+str(n)+":/varys/trace.txt"
		#info(cmd)
		returnCode = os.system(cmd)
		infostr = "** Trace file copied to mn.m"+str(n)+"\n"
		info(infostr)	
		# transmission
		slavecmd2 = "nohup ./run coflowemu.services.EmulationSlave coflowemu://10.0.0.254:1606 /varys/trace.txt "+ str(n)    		
		host.popen(slavecmd2)

	CLI(net)

	net.stop()

def SparkTest(net = None, clusters = None):
    
    net.start()
    info('** Waiting for switches to connect to the controller\n')
    sleep(2)
    hosts = net.hosts   

    info('clusters init...\n')
    i = 0
    c_index = 0     #record index of cluster

    #init cluster
    for cluster in clusters:
        c_index += 1
        info ('***cluster %s init***\n '%str(c_index)) 
        
        slaves = cluster['slaves']
        parallelism = cluster['parallelism']
        master = ''
        #file name
        host_file = 'hosts-cluster%s.txt'%str(c_index)
        slaves_file = 'slaves-cluster%s.txt'%str(c_index)

        for n in range(slaves+1):          
            #copy hosts file 
            host = hosts[i]
            i += 1

            cmd = "sudo docker cp " + host_file + " mn.%s:/usr/hosts.txt"%host.name
            returnCode = os.system(cmd)

            infostr = "** Hosts file copied to %s **\n"%host.name
            info(infostr)	

            #if master
            if n == 0 :
                cmd = "sudo docker cp " + slaves_file + " mn.%s:/usr/slaves.txt"%host.name
                returnCode = os.system(cmd)
                master = host.name 

                infostr = "** Slaves file copied to %s\n"%host.name
                info(infostr)	

                #start ssh service
                dockercmd = 'service ssh start'
                ret = host.cmd(dockercmd)
                ret = ret + '\n'
                info(ret) 

                #modify hdfs master in hibench hadoop.conf
                dockercmd = "sed -i 's/master:9000/%s:9000/' /usr/hibench/HiBench/conf/hadoop.conf"%master
                ret = host.cmd(dockercmd)

                #modify spark master in hibench spark.conf
                dockercmd = "sed -i 's/master:7077/%s:7077/' /usr/hibench/HiBench/conf/spark.conf"%master
                ret = host.cmd(dockercmd)

                #modify parallelism in hibench hibench.conf
                parastr = "hibench.default.map.parallelism " + str(parallelism)
                dockercmd = "sed -i '/^hibench.default.map.parallelism/c%s' /usr/hibench/HiBench/conf/hibench.conf"%parastr
                ret = host.cmd(dockercmd)

                parastr = "hibench.default.shuffle.parallelism " + str(parallelism)
                dockercmd = "sed -i '/^hibench.default.shuffle.parallelism/c%s' /usr/hibench/HiBench/conf/hibench.conf"%parastr
                ret = host.cmd(dockercmd)


            #if slave 
            else:
                dockercmd = '/usr/init-slave.sh'
                ret = host.cmd(dockercmd)    
                ret = ret + '\n'   
                info(ret)
            
            #modify configuration
            #modify hdfs master
            dockercmd = "sed -i 's/master:9000/%s:9000/' /usr/hadoop-2.8.3/etc/hadoop/core-site.xml"%master
            ret = host.cmd(dockercmd)

            #modify yarn master
            dockercmd = "sed -i 's/master:/%s:/' /usr/hadoop-2.8.3/etc/hadoop/yarn-site.xml"%master
            ret = host.cmd(dockercmd)


    #start spark
    i = 0 
    c_index = 0 

    for cluster in clusters:
        c_index += 1
        info ('***start spark in cluster %s***\n '%str(c_index)) 
        
        slaves = cluster['slaves']
        master = ''
        for n in range(slaves+1): 
            host = hosts[i]
            i += 1
            #start master
            if n == 0 :
                info('start spark in %s\n'%host.name)
                master = host.name
                startspark = '/usr/start-master.sh'
                ret = host.cmd(startspark)
                ret = ret + '\n'
                info(ret)
            
            #start slave
            else:
                info('start slaves in %s\n'%host.name)
                startslave = '/usr/spark-2.3.0/sbin/start-slave.sh -h %s spark://%s:7077'%(host.name, master)
                ret = host.cmd(startslave)   
                ret = ret + '\n'    
                info(ret)               

    info('Done...hang up...\n')
        
    try:
        while True:
            sleep(1)
    except KeyboardInterrupt:
        info('End by KeyboardInterrupt...\n')
    finally:
        CLI(net)
        net.stop()


if __name__ == '__main__':
    setLogLevel( 'info' )

    json_file = open("config.json")
    config = json.load(json_file)
    json_file.close()
    print (config)
    #sys.exit()

    topology = config["networkTopology"]

    if topology == "CoflowEmuNet":
        net = CoflowEmuNet(node_count=config["numNodes"],
            bw=config["bandwidthMininet"],
    #			cpu=config["cpuLimit"],
                        #cpu=0.05,
    #		queue=config["queue"],
            remoteController=config["remoteController"])
        RunTest(net=net, remoteController=config["remoteController"], trace=config["trace"])
		# generate Trace.txt
	#	Trace(config['numNodes'],config['numCoflows'])
    elif topology == "DockerSpark":
        net= DockerSparkNet(cluster_count=1, bw=config["bandwidthMininet"], remoteController=config["remoteController"],  clusters=config['clusters'], image = config['image'])
        SparkTest(net=net,clusters = config['clusters'])
        
    else:
        print ("Error: Unknown topology type.")
        sys.exit(1)

	

	os.system('sudo mn -c')

