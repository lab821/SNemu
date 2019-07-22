from mininet.topo import Topo
from mininet.node import Controller, RemoteController, OVSKernelSwitch, CPULimitedHost, OVSController, Docker
from mininet.net import Containernet, Mininet
from mininet.link import TCLink
from mininet.cli import CLI
from mininet.util import custom, irange
from mininet.log import setLogLevel, info, warn, error, debug
from mininet.topo import LinearTopo
from mininet.topolib import TreeTopo

from topology import FatTreeTopo, NonBlockingTopo, LinearMultipathTopo

from subprocess import Popen, PIPE
from argparse import ArgumentParser
import multiprocessing
from time import sleep
#from monitor.monitor import monitor_devs_ng
import os
import sys
import json
from sflow import wrapper

#from simulator import simulator

# Number of pods in Fat-Tree
K = 4

# Queue Size
QUEUE_SIZE = 100

# Link capacity (Mbps)
BW = 10

def CoflowEmuNet(node_count=16, bw=BW, cpu=-1, queue=100, remoteController=False):
    "Create an empty network and add nodes to it."
    #setattr(Mininet, 'start', wrapper(Mininet.__dict__['start'], '127.0.0.1'))
    param = {}
    execfile('./sflow.py',param,param)
    for name,val in param.iteritems():
        globals()[name] = val
    host = custom(Docker, dimage="emulator:cn")
    #link = custom(TCLink, bw=bw, max_queue_size=queue, delay="1ms")
    link = custom(TCLink, bw=bw)
    if remoteController is True:
        controller = RemoteController
    else:
        controller = Controller
    net = Containernet(host=host, link=link, switch=OVSKernelSwitch, controller=controller, autoSetMacs=True, autoStaticArp=False)

    info('*** Adding controller\n')
    if remoteController is True:
        net.addController('c0',ip='127.0.0.1',port=6653)
    else:
        net.addController( 'c0' )

    info('*** Adding hosts\n')
    allHosts = []
    i = 1

    info('*** Adding master\n')
    host = net.addDocker("master", ip='10.0.0.254', dimage="emulator:cn", environment=["EMU_LOCAL_IP=10.0.0.254"], ports=[16016], port_bindings={16016:19999})
   #host.sendCmd("./run varys.framework.master.EmulationMaster --ip 10.0.0.254 --port 1606 --trace /varys/core/src/main/resources/traces/TestTrace.txt")
    #debug
    #print host.dcinfo

    i = i + 1
    allHosts.append(host)

    info('*** Adding nodes\n')
    # Adding hosts
    for n in irange(1, node_count):
        print ("m%s" % n)
    	nodeip = "10.0.0.%s" % str(i)
        host = net.addHost("m%s" % n, ip=nodeip, dimage="emulator:cn", network_mode="none", environment=["EMU_LOCAL_IP=%s" % nodeip])
        # host.sendCmd("./bin/start-slave.sh varys://10.0.0.254:1606")
        # cmdstr = "./run varys.examples.EmulationSlave varys://10.0.0.254:1606 /varys/core/src/main/resources/traces/TestTrace.txt "+str(n)
        # host.sendCmd(cmdstr)
        i = i + 1
        allHosts.append(host)

    info('*** Adding switches\n')
    i = 1
    # Adding switches
    print ("s%s" % i)
    switch = net.addSwitch("s%s" % i)

    info('*** Creating links to hosts\n')
    # Creating links to hosts
    for n in irange(0, len(allHosts)-1):
        host = allHosts[n]
        print ("linking %s to %s" % (host.name, switch.name))
        net.addLink(host, switch)
        print ("")

    return net


def DockerSparkNet(cluster_count=1, bw=BW, remoteController=False, clusters=None, image = None):
    param = {}
    execfile('./sflow.py',param,param)
    for name,val in param.iteritems():
        globals()[name] = val

    host = custom(Docker, dimage=image)
    #link = custom(TCLink, bw=bw, max_queue_size=queue, delay="1ms")
    link = custom(TCLink, bw=bw)
    if remoteController is True:
        controller = RemoteController
    else:
        controller = Controller
    net = Containernet(host=host, link=link, switch=OVSKernelSwitch, controller=controller, autoSetMacs=True, autoStaticArp=False)

    info('*** Adding controller\n')
    if remoteController is True:
        net.addController('c0',ip='127.0.0.1',port=6653)
    else:
        net.addController( 'c0' )

    info('*** Adding hosts\n')
    allHosts = []
    i = 1       #record number of hosts
    c_index = 0     #record index of cluster
    cpu_index = 0   #record the min index of cpu available

    # adding clusters 
    for cluster in clusters:

        # init config
        cores = cluster['cores']
        slaves = cluster['slaves']
        memory = cluster['memory']
        c_index += 1
        node_index = 1
        info ('*** Adding cluster %s \n '%str(c_index))

        #file name
        host_file = 'hosts-cluster%s.txt'%str(c_index)
        slaves_file = 'slaves-cluster%s.txt'%str(c_index)

        #port binding
        port_bindings = {}
        port_bindings[7077] = 7077 + 100*c_index
        port_bindings[8080] = 8080 + 100*c_index
        port_bindings[18080] = 18080 + 100*c_index
        port_bindings[8088] = 8088 + 100*c_index

        with open(host_file,'w') as file:
            # adding master
            
            file.write("127.0.0.1   localhost\n")
            
            #cpu set
            cpuset = '%s-%s'%(str(cpu_index),str(cpu_index + cores - 1))
            cpu_index += cores 

            nodeip = "172.%s.0.%s" %(str(c_index), str(node_index+1)) 
            host = net.addDocker("master%s"%str(c_index), ip=nodeip, dimage=image, ports=[7077,8080,8088,18080], port_bindings=port_bindings, cpuset_cpus=cpuset, mem_limit=memory)

            file.write(nodeip + "   master%s\n"%str(c_index))
            
            i = i + 1
            node_index += 1
            allHosts.append(host)

            #adding slaves
            with open(slaves_file,'w') as sf:
                for n in range(slaves):
                    info ("slave%s-%s" % (str(c_index),str(n+1)))

                    #cpu set
                    cpuset = '%s-%s'%(str(cpu_index),str(cpu_index + cores - 1))
                    cpu_index += cores 

                    nodeip = "172.%s.0.%s" %(str(c_index), str(node_index+1)) 
                    host = net.addHost("slave%s-%s" % (str(c_index),str(n+1)), ip=nodeip, dimage=image, network_mode="none", cpuset_cpus=cpuset, mem_limit=memory)

                    file.write(nodeip + "    slave%s-%s\n"% (str(c_index),str(n+1)))
                    sf. write("slave%s-%s\n"% (str(c_index),str(n+1))) 
                    
                    i = i + 1
                    node_index += 1

                    allHosts.append(host)
    #adding completed

    info('*** Adding switches\n')
    i = 1
    
    # Adding switches
    info ("s%s" % str(i))
    switch = net.addSwitch("s%s" % str(i))

    info('*** Creating links to hosts\n')
    
    # Creating links to hosts
    for n in irange(0, len(allHosts)-1):
        host = allHosts[n]
        print ("linking %s to %s" % (host.name, switch.name))
        net.addLink(host, switch)
        print ("")

    return net


