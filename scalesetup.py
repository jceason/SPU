#!/usr/bin/python

import os
import re
import sys
import argparse
import subprocess
import time

INSTALLHOME = '/root/SPU'
metadata = {}
metadata["nsd_node_count"] = 2
metadata["nsd_node_prefixname"] = "sn" 
metadata["systempool_disk_count"] = 0
metadata["silverpool_disk_count"] = 0
metadata["scratch_disk_count"] = 0
metadata["client_node_count"] = 3 
metadata["client_node_prefixname"] = "cn" 
metadata["util_node_count"] = 0
metadata["clustername"] = "mycluster"
metadata["guip"] = "gpfsstorage123"
metadata["fs_name"] = "fs1"  


sys.path.append(INSTALLHOME + '/lib/python')
import gpfsthread
from gpfsthread import * 

advanced = True
byol = None


INSTALLERLIMIT = 16

numNSDdisks = 0

parser = argparse.ArgumentParser()

parser.add_argument("--install",action="store_true")
parser.add_argument("--deploymanager",action="store_true")
parser.add_argument("--configure",action="store_true")
parser.add_argument("--version",action="store",dest="version")

options = parser.parse_args()


if options.install:
   if not options.version:
       print "You must specify --version argument for install.  Example --version 5.0.3.1"
       sys.exit()
   if not os.path.isdir("/usr/lpp/mmfs/" + options.version):
       print "Error failed to find Spectrum Scale software in /usr/lpp/mmfs/" + options.version
       sys.exit()
   else: 
       print "Installing Spectrum Scale"
elif options.deploymanager: 
   print "Startup script from Deployment manager"
elif options.configure: 
   if not os.path.isdir("/usr/lpp/mmfs/bin"):
       print "Error Spectrum Scale software is not installed "
       sys.exit()
   else: 
       print "Configuring Spectrum Scale"
else: 
   print "Error no options specified \n"
   print "Usage: \n"
   print "scalesetup.py --install --configure --version \n" 
   print "  --install   install software on all nodes \n" 
   print "  --configure configure the Spectrum Scale cluster \n" 
   print "  --version the Spectrum Scale version to install.  Example 5.0.3.1 \n" 
   sys.exit()

## Check if cluster is already installed
exists = os.path.isfile("/var/mmfs/gen/mmsdrfs")
if exists:
   print "IBM Spectrum Scale is already installed and configured \n"
   sys.exit()

##Add GPFS to path
scalepath = "/usr/lpp/mmfs/bin"
os.environ["PATH"] += os.pathsep + scalepath 

timestr = time.strftime("%Y%m%d-%H%M%S")
run_cmd("mkdir -p " + INSTALLHOME + "/logs") 
LOGP = open(INSTALLHOME + "/logs/scale." + timestr + ".log" , "w") 

def update_status (message) :
    STATUSP = open(INSTALLHOME + "/INSTALL-STATUS", "w") 
    STATUSP.write(message + "\n")
    STATUSP.close()
    timestr = time.strftime("%Y%m%d-%H%M%S")
    LOGP.write(timestr + " " +  message + "\n")

## runs a command until successful
def wait_cmd (testcmd) :
    print testcmd
    timestr = time.strftime("%Y%m%d-%H%M%S")
    out = ""
    success = 0
    while  success != 1: 
       LOGP.write(timestr +" CMD:" + testcmd + "\n")
       try:
           success = 1
           out = subprocess.check_output(testcmd,stderr=subprocess.STDOUT,shell=True)
       except subprocess.CalledProcessError as exc:
          success = 0
          time.sleep(5)
          pass 

       LOGP.write(out)
    return out.rstrip('\n');

#################################
#################################
def run_cmd (testcmd,retry = 0) :
     
    out = ""
    retry += 1 ;
    while  retry > 0:

        print testcmd
        retry -= 1
        timestr = time.strftime("%Y%m%d-%H%M%S")
        LOGP.write(timestr +" CMD:" + testcmd + "\n")
        try:
          out = subprocess.check_output(testcmd,stderr=subprocess.STDOUT,shell=True)
          retry = 0
        except subprocess.CalledProcessError as exc:
          LOGP.write(str(exc.returncode) +  exc.output)
          timestr = time.strftime("%Y%m%d-%H%M%S")

          if retry < 1:
              update_status(timestr + " fail \n" + testcmd + str(exc.returncode) + exc.output)
              LOGP.close()
              raise
          else:
              LOGP.write("Retry " + testcmd + "  in 5 sec")
              time.sleep(5)

    LOGP.write(out)
    return out.rstrip('\n');
#################################

#################################
#################################
def gpfs_cmd (testcmd,retry = 0) :
    out = ""
    if byol is None:
      retry += 1 ;
      while  retry > 0:

          print testcmd
          retry -= 1
          timestr = time.strftime("%Y%m%d-%H%M%S")
          LOGP.write(timestr +" CMD:" + testcmd + "\n")
          try:
            out = subprocess.check_output(testcmd,stderr=subprocess.STDOUT,shell=True)
            retry = 0
          except subprocess.CalledProcessError as exc:
            LOGP.write(str(exc.returncode) +  exc.output)
            timestr = time.strftime("%Y%m%d-%H%M%S")

            if retry < 1:
                update_status(timestr + " fail \n" + testcmd + str(exc.returncode) + exc.output)
                LOGP.close()
                raise
            else:
                LOGP.write("Retry " + testcmd + "  in 5 sec")
                time.sleep(5)

    LOGP.write(out)
    return out.rstrip('\n');
#################################



##INSTALL START
run_cmd("date -R")
update_status("inprogress \n")

#Example information
#byol true
#client_node_count 0
#client_node_prefixname cn
#clustername sc1
#configName testcluster-startup-config
#enable-guest-attributes TRUE
#enable_advanced true
#fs_name fs1
#mgmt_node_name mn1
#util_node_count 0
#util_node_prefixname tctn
#nsd_node_count 2
#nsd_node_prefixname sn
#scratch_disk_count 1
#scratch_disk_type local-ssd
#silverpool_disk_count 0
#silverpool_disk_type pd-standard
#systempool_disk_count 0
#systempool_disk_type pd-ssd

mgmtname = run_cmd("hostname")
mgmtip = run_cmd("hostname -I")
#



##NSD Nodes
##for cluster creation
NODEP = open("/tmp/scalesnodes", "w") 
##Just a list of server nodes
SERVERP = open("/tmp/servernodes", "w") 
NODEA = [] 

##Client nodes
CNODEP = open("/tmp/clientnodes", "w") 
CNODEA = [] 

##Util nodes
UNODEP = open("/tmp/utilnodes", "w") 
UNODEA = [] 

DISKP = open("/tmp/nsddisks", "w") 

NODEP.write(mgmtname + ":quorum\n")

if byol:
   BYOLP.write("spectrumscale setup -s " + mgmtip + "\n")  
   BYOLP.write("spectrumscale config perfmon -r on \n")
   BYOLP.write("spectrumscale callhome disable \n")
   BYOLP.write("spectrumscale node add " + mgmtname +" -m -q -g \n")


for n in range(1,int(metadata["nsd_node_count"])+1):
    managerquorum = "" 
    byolmanagerquorum = "" 
    nodename = metadata["nsd_node_prefixname"] + str(n) 
    NODEA.append(nodename)

    ##need to avoid having 3 nsd servers and 1 mgmt .. which is 4 quorum 
    if n < 5 :
       if  not (n == 3 and int(metadata["nsd_node_count"]) == 3): 
           byolmanagerquorum = " -m -q" 
           managerquorum = ":manager-quorum"


    ##This is the NSD serves and if they are also a manager and quorum node 
    NODEP.write(nodename + managerquorum + "\n")
    SERVERP.write(nodename + "\n")
    if byol:
       BYOLP.write("spectrumscale node add " + nodename + " -n " + byolmanagerquorum  + " \n") 
   
    devicecnt = 1 
    for d in range(1,int(metadata["systempool_disk_count"])+1):
        numNSDdisks += 1
        deviceid  =  "/dev/sd" + str(chr(ord('a')+ devicecnt))
        if byol:
           BYOLP.write("spectrumscale nsd add -p " + nodename + " -fs " + metadata["fs_name"] + " -po system -u dataAndMetadata -fg " +  str(n) + " " + deviceid + " \n")

        DISKP.write("%nsd: nsd=" + nodename + "nsd" + str(devicecnt) + " device=" + deviceid + "\n")
        DISKP.write("  servers=" + nodename + "\n")
        DISKP.write("  usage=dataAndMetadata\n")
        DISKP.write("  failureGroup=" + str(n) + "\n")
        DISKP.write("  pool=system\n")
        devicecnt += 1

    for d in range(1,int(metadata["silverpool_disk_count"])+1):
        numNSDdisks += 1
        deviceid  =  "/dev/sd" + str(chr(ord('a')+ devicecnt))
        if byol:
           BYOLP.write("spectrumscale nsd add -p " + nodename + " -fs " + metadata["fs_name"] + " -po silver -u dataOnly -fg " +  str(n) + " " + deviceid + " \n")
        DISKP.write("%nsd: nsd=" + nodename + "nsd" + str(devicecnt) + " device=" + deviceid + "\n")
        DISKP.write("  servers=" + nodename + "\n")
        DISKP.write("  usage=dataOnly\n")
        DISKP.write("  failureGroup=" + str(n) + "\n")
        DISKP.write("  pool=silver\n")
        devicecnt += 1

    for d in range(1,int(metadata["scratch_disk_count"])+1):
        numNSDdisks += 1
        deviceid  =  "/dev/sd" + str(chr(ord('a')+ devicecnt))
        if byol:
           BYOLP.write("spectrumscale nsd add -p " + nodename + " -fs scratch -po system -u dataAndMetadata -fg " +  str(n) + " " + deviceid + " \n")
        DISKP.write("%nsd: nsd=" + nodename + "nsd" + str(devicecnt) + " device=" + deviceid + "\n")
        DISKP.write("  servers=" + nodename + "\n")
        DISKP.write("  usage=dataAndMetadata\n")
        DISKP.write("  failureGroup=" + str(n) + "\n")
        devicecnt += 1


 
##Deploy NSD serves and management node first
##Installer will run OOM if we try to deploy a large cluster
if byol:
   BYOLP.write("spectrumscale config gpfs -c " + metadata["clustername"] + " -p default -r /usr/bin/ssh -rc /usr/bin/scp \n")
   BYOLP.write("spectrumscale install \n")
   BYOLP.write("spectrumscale deploy \n")
   ##Need to give the GUI a user and  password
   BYOLP.write("/usr/lpp/mmfs/gui/cli/mkuser admin -g SecurityAdmin -p " + metadata["guip"] + " \n")

##Installer work around
##This is used to prevent large groups of clients to be added and causing chef to OOM
installerLimit  = INSTALLERLIMIT 

for n in range(1,int(metadata["client_node_count"])+1):
    CNODEP.write(metadata["client_node_prefixname"] + str(n) + "\n")
    CNODEA.append(metadata["client_node_prefixname"] + str(n))
    if byol:
       BYOLP.write("spectrumscale node add " + metadata["client_node_prefixname"] + str(n) + " \n")
       installerLimit -= 1 
       if installerLimit == 0:
           BYOLP.write("spectrumscale install \n")
           BYOLP.write("spectrumscale deploy \n")
           installerLimit  = INSTALLERLIMIT 

for n in range(1,int(metadata["util_node_count"])+1):
    UNODEP.write(metadata["util_node_prefixname"] + str(n) + "\n")
    UNODEA.append(metadata["util_node_prefixname"] + str(n))
    if byol:
       BYOLP.write("spectrumscale node add " + metadata["util_node_prefixname"] + str(n) + " \n") 
       installerLimit -= 1 
       if installerLimit == 0:
           BYOLP.write("spectrumscale install \n")
           BYOLP.write("spectrumscale deploy \n")
           installerLimit  = INSTALLERLIMIT 

if byol:
   ##We have some clients but did not just put these commands 
   if installerLimit < INSTALLERLIMIT:
      BYOLP.write("spectrumscale install \n")
      BYOLP.write("spectrumscale deploy \n")

DISKP.close()
NODEP.close()
SERVERP.close()
CNODEP.close()
UNODEP.close()

domainname = run_cmd("dnsdomainname")
print "length is:" + str(len(domainname))
if len(domainname) > 0 :
   domainname = "." + dnsdomainname

## Create new keys and exchange
if options.deploymanager:
   ##Make new ssh keys
   run_cmd("mkdir -p " + INSTALLHOME  + "/oldkeys") 
   run_cmd("cp -r /root/.ssh/* " + INSTALLHOME + "/oldkeys")                 
   run_cmd("mkdir -p " + INSTALLHOME + "/newkeys")    
   run_cmd("ssh-keygen -t rsa -N '' -f " + INSTALLHOME + "/newkeys/id_rsa")            
   run_cmd("cp " + INSTALLHOME + "/newkeys/id_rsa.pub " + INSTALLHOME + "/newkeys/authorized_keys")


   ## Mgmt node must wait to make sure other nodes are alive
   ## and able to write new keys
   ## use wait_cmd to make sure scp retries for node to be up
   for n in NODEA:
       wait_cmd("scp -r -o StrictHostKeyChecking=no " + INSTALLHOME + "/newkeys/* " + n + ":/root/.ssh") 

   for n in CNODEA:
       wait_cmd("scp -r -o StrictHostKeyChecking=no " + INSTALLHOME + "/newkeys/* " + n + ":/root/.ssh") 

   for n in UNODEA:
       wait_cmd("scp -r -o StrictHostKeyChecking=no " + INSTALLHOME + "/newkeys/* " + n + ":/root/.ssh") 

   run_cmd("cp " + INSTALLHOME + "/newkeys/* /root/.ssh") 

   #known hosts for mgmt
   run_cmd("ssh -q -o StrictHostKeyChecking=no " + mgmtname + " hostname")
   run_cmd("ssh -q -o StrictHostKeyChecking=no " + mgmtname + domainname  + " hostname") 

   #known hosts for nsd servers
   for n in NODEA:
       run_cmd("ssh -q -o StrictHostKeyChecking=no " + n + " hostname") 
       run_cmd("ssh -q -o StrictHostKeyChecking=no " + n + domainname + " hostname") 

if options.install:
   try: 
       ## more efficient to feed in as one node array
       allNodes=[]
       allNodes.append(mgmtname)
       allNodes = allNodes + CNODEA 
       allNodes = allNodes + UNODEA 
       allNodes = allNodes + NODEA
       gpfsthread.installNodes(allNodes,"base",options.version)
       print "Install Complete"
   except Exception as err:
       LOGP.close()
       raise

## Do now continue unless this is launched by deployment manager
## or user has not asked to configure
if not options.configure and not options.deploymanager:
   print "Configuration not requested.  Exiting"
   LOGP.close()
   sys.exit()

print "Starting configuration\n"

#create cluster
gpfs_cmd("mmcrcluster -N /tmp/scalesnodes -A -r /usr/bin/ssh -R /usr/bin/scp --ccr-enable -C " + metadata["clustername"]); 

#Licenses
gpfs_cmd("mmchlicense server --accept -N " + mgmtname); 

if int(metadata["nsd_node_count"]) > 0 :
   gpfs_cmd("mmchlicense server --accept -N /tmp/servernodes"); 


##GUI setup
gpfs_cmd("systemctl enable gpfsgui.service"); 

##Perfmon setup
gpfs_cmd("mmperfmon config generate --collectors " + mgmtname ); 
gpfs_cmd("mmchnode --perfmon -N " + mgmtname ) 

if int(metadata["nsd_node_count"]) > 0 :
   gpfs_cmd("mmchnode --perfmon -N /tmp/servernodes"); 

gpfs_cmd("systemctl enable pmcollector.service"); 
gpfs_cmd("systemctl start pmcollector.service"); 

gpfs_cmd("systemctl enable pmsensors.service"); 
gpfs_cmd("systemctl start pmsensors.service"); 

if byol is None and int(metadata["nsd_node_count"]) > 0:
   gpfsthread.enablePerfNodes(NODEA)
   print "NSD Node Perf Complete"

   ## Configure NSD perf parameters
   gpfs_cmd("mmchconfig workerThreads=512 -N /tmp/servernodes" )
   firstNSDnodename = metadata["nsd_node_prefixname"] + "1"

   out = run_cmd("ssh -q -o StrictHostKeyChecking=no " +  firstNSDnodename + " free total -m | grep Mem:")
   fields = out.split()
   memorymB = int(fields[1])
   if memorymB > 6000:
      psize = int(round((memorymB  * .20)/256)) * 256
      gpfs_cmd("mmchconfig pagepool=" + str(psize) + "M -N /tmp/servernodes")

##Get NSD and MGMT node healthy first
gpfs_cmd("mmchconfig workerThreads=512" )
gpfs_cmd("mmchconfig numaMemoryInterleave=yes"); 
gpfs_cmd("mmchconfig maxStatCache=0"); 
gpfs_cmd("mmchconfig enableLowspaceEvents=yes")
#gpfs_cmd("mmshutdown -a"); 
#gpfs_cmd("mmchconfig maxblocksize=16384K")

gpfs_cmd("mmstartup -a"); 
gpfs_cmd("systemctl start gpfsgui "); 

# gpfs_cmd("/usr/lpp/mmfs/gui/cli/mkuser admin -g SecurityAdmin -p " + metadata["guip"] , 2)


##Must have cluster active to create filesystem
##And add nodes 
if byol is None:
  active = 0
  time.sleep(1)
  while  active != 1:
     out = gpfs_cmd("mmgetstate -Y -a")
     if re.search(r':down:',out):
        print  "cluster node is down"
        active = 0
        time.sleep(5)
     elif re.search(r':unknown:',out):
        print  "cluster node is unknown"
        active = 0
        time.sleep(5)
     elif re.search(r':active:',out):
        print  "cluster is active"
        active = 1
     else:
        print  "Did not detect status"
        active = 0
        time.sleep(5)

if numNSDdisks > 0:
   gpfs_cmd("mmcrnsd -F /tmp/nsddisks " , 2); 

## if we have clients
if int(metadata["client_node_count"]) > 0 or int(metadata["util_node_count"]) > 0:

  ##must populate known hosts
  for n in CNODEA:
    run_cmd("ssh -q -o StrictHostKeyChecking=no " + n + " hostname"); 
    run_cmd("ssh -q -o StrictHostKeyChecking=no " + n + domainname + " hostname"); 

  for n in UNODEA:
    run_cmd("ssh -q -o StrictHostKeyChecking=no " + n + " hostname"); 
    run_cmd("ssh -q -o StrictHostKeyChecking=no " + n + domainname + " hostname"); 

  ## copy known hosts to storage nodes 
  for n in NODEA:
    run_cmd("scp /root/.ssh/known_hosts " + n + ":/root/.ssh ") 

  ## copy known hosts to client and util nodes 
  for n in CNODEA:
    run_cmd("scp /root/.ssh/known_hosts " + n + ":/root/.ssh "); 

  for n in UNODEA:
    run_cmd("scp /root/.ssh/known_hosts " + n + ":/root/.ssh "); 

## Client perf config
if int(metadata["client_node_count"]) > 0 and byol is None: 
   gpfs_cmd("mmaddnode -N /tmp/clientnodes"); 
   gpfs_cmd("mmchlicense client --accept -N /tmp/clientnodes"); 
   gpfs_cmd("mmchnode --perfmon -N /tmp/clientnodes"); 

   gpfsthread.enablePerfNodes(CNODEA)
   print "Client Node Perf Complete"

   ## Configure Client perf parameters
   gpfs_cmd("mmchconfig workerThreads=512 -N /tmp/clientnodes" )
   gpfs_cmd("mmchconfig maxFilesToCache=128k -N /tmp/clientnodes" )

   firstClientnodename = metadata["client_node_prefixname"] + "1"
   out = run_cmd("ssh -q -o StrictHostKeyChecking=no " +  firstClientnodename + " free total -m | grep Mem:")
   fields = out.split()
   memorymB = int(fields[1])
   if memorymB > 6000:
      psize = int(round((memorymB  * .20)/256)) * 256
      ##cap at 8GB
      if psize > 8192: 
         psize = 8192
      gpfs_cmd("mmchconfig pagepool=" + str(psize) + "M -N /tmp/clientnodes")

## Util perf config
if int(metadata["util_node_count"]) > 0 and byol is None:
   gpfs_cmd("mmaddnode -N /tmp/utilnodes"); 
   gpfs_cmd("mmchlicense client --accept -N /tmp/utilnodes"); 
   gpfs_cmd("mmchnode --perfmon -N /tmp/utilnodes"); 

   gpfsthread.enablePerfNodes(UNODEA)
   print "Secondary Client Node Perf Complete"

   ## Configure utility nodes  perf parameters
   gpfs_cmd("mmchconfig workerThreads=512 -N /tmp/utilnodes" )
   gpfs_cmd("mmchconfig maxFilesToCache=128k -N /tmp/utilnodes" )

   firstUtilnodename = metadata["util_node_prefixname"] + "1"
   out = run_cmd("ssh -q -o StrictHostKeyChecking=no " +  firstUtilnodename + " free total -m | grep Mem:")
   fields = out.split()
   memorymB = int(fields[1])
   if memorymB > 6000:
      psize = int(round((memorymB  * .20)/256)) * 256
      ##cap at 8GB
      if psize > 8192: 
         psize = 8192
      gpfs_cmd("mmchconfig pagepool=" + str(psize) + "M -N /tmp/utilnodes")



#startup client and util nodes
gpfs_cmd("mmstartup -a"); 


## Only create filesystem and policy if this is not an advanced install
if advanced is None and numNSDdisks > 0:
   gpfs_cmd("mmcrfs " + metadata["fs_name"] + " -F /tmp/nsddisks " + " -T /gpfs/" + metadata["fs_name"] + " -m 2 -A yes -D nfs4"); 

   gpfs_cmd("mmmount all -a"); 



update_status("complete \n")
## Backup cluster configuration
gpfs_cmd("mmlslicense --capacity")

update_status("complete \n")

LOGP.close()

