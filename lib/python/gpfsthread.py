#!/usr/bin/python

import os
import re
import sys
import subprocess
import time
import logging
from threading import Thread

logging.basicConfig(level=logging.WARNING,
                    format='[%(levelname)s] (%(threadName)-10s) %(message)s',
                    )

## Need to have some limit on number of nodes to install at same time
threadLimit = 32 
## Dict to keep track if thread on a node has a problem 
threadsERR = {}

##Install dir and script
installDir = "/tmp/gpfs_rpms"
script = installDir + "/install.sh"

def run_cmd (testcmd) :
    out = ""
    logging.debug('Running ' + testcmd)
    try:
      out = subprocess.check_output(testcmd,stderr=subprocess.STDOUT,shell=True)
    except subprocess.CalledProcessError as exc:
      raise
    return out.rstrip('\n');

def waitforThreads(threads):
    logging.debug('Waiting for threads to finish')
    for thread in threads:
        thread.join()
    del threads[:]

def installScript(script):
    IP = open(script, "w")
    IP.write("cd " + installDir + "  \n")
    IP.write("yum install *.rpm -y\n")
    IP.write("/usr/lpp/mmfs/bin/mmbuildgpl \n")
    IP.close()
    run_cmd("chmod +x " + script)

def installPackage(package,version):
    basepath = "/usr/lpp/mmfs/"+ version
    try:
        run_cmd("mkdir -p " + installDir)
        run_cmd("cp " + basepath + "/gpfs_rpms/gpfs.gui*.rpm " + installDir)
        run_cmd("cp " + basepath + "/gpfs_rpms/gpfs.java*.rpm " + installDir)
        run_cmd("cp " + basepath + "/gpfs_rpms/gpfs.compression*.rpm " + installDir)
        run_cmd("cp " + basepath + "/gpfs_rpms/gpfs.msg*.rpm " + installDir)
        run_cmd("cp " + basepath + "/gpfs_rpms/gpfs.base*.rpm " + installDir)
        run_cmd("cp " + basepath + "/gpfs_rpms/gpfs.gpl*.rpm " + installDir)
        run_cmd("cp " + basepath + "/gpfs_rpms/gpfs.gskit*.rpm " + installDir)
        run_cmd("cp " + basepath + "/gpfs_rpms/gpfs.docs*.rpm " + installDir)
        run_cmd("cp " + basepath + "/gpfs_rpms/gpfs.license*.rpm " + installDir)
        run_cmd("cp " + basepath + "/gpfs_rpms/gpfs.callhome*.rpm " + installDir)

        run_cmd("cp " + basepath + "/gpfs_rpms/rhel7/gpfs.callhome*.rpm " + installDir)
        run_cmd("cp " + basepath + "/gpfs_rpms/rhel7/gpfs.kafka*.rpm " + installDir)
        run_cmd("cp " + basepath + "/gpfs_rpms/rhel7/gpfs.librdkafka*.rpm " + installDir)

        run_cmd("cp " + basepath + "/zimon_rpms/rhel7/gpfs.gss.pmcollector*.rpm " + installDir)
        run_cmd("cp " + basepath + "/zimon_rpms/rhel7/gpfs.gss.pmsensors*.rpm " + installDir)

    except Exception as err:
        logging.error('Error building package')
        raise

def installNode(node,packagegroup):
    try:
        print "Installing software on " + node + " " + packagegroup
        logging.debug('Installing Start Node ' + node)
        run_cmd("/usr/bin/scp -r -o StrictHostKeyChecking=no " + installDir  + " " + node + ":/tmp");
        run_cmd("/usr/bin/ssh -q -o StrictHostKeyChecking=no " + node + " " + script );
        logging.debug('Installing Complete Node ' + node)
    except Exception as err:
        message = "Installing Error Node " + str(node)
        logging.error(message)
        threadsERR[node] = err
        raise
    

def perfNode(node,nodetype):
    try:
        logging.debug('Setup Perf Node ' + node)
        run_cmd("/usr/bin/ssh -q -o StrictHostKeyChecking=no " + node + " systemctl enable pmsensors.service"); 
        run_cmd("/usr/bin/ssh -q -o StrictHostKeyChecking=no " + node + " systemctl start pmsensors.service");

        logging.debug('Performance Monitors complete on ' + node)
    except Exception as err:
        message = "Performance Monitors  Setup Error Node " + str(node)
        logging.error(message)
        threadsERR[node] = err
        raise

def runOnNodes(NODEA,opt,func):
    rc = 0
    threads = []
    for nodename in NODEA:
        t = Thread(target=func, args=[nodename,opt])
        threads.append(t)
        t.start() 
        if len(threads)%threadLimit == 0:
            waitforThreads(threads)
    waitforThreads(threads)
    for node in threadsERR: 
        logging.error('Failed setupPerf on ' + node + " : " + str(threadsERR[node]))
        rc += 1

    return rc 


def enablePerfNodes(NODEA):
    return runOnNodes(NODEA,"",perfNode) 

def installNodes(NODEA,packagegroup,version):
    installPackage(packagegroup,version)
    installScript(script)
    return runOnNodes(NODEA,packagegroup,installNode) 
