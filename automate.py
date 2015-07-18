#!/usr/bin/python2

import os
import commands
import getpass
import thread
#import socket

#host=commands.getoutput("ifconfig  eth0 | grep 192 | awk '{print$2}'|cut -c 6-21")
#port=2020
#sock=socket.socket(socket.AF_INET,socket.SOCK_DGRAM,0)#open socket
#sock.bind((host,port))


def namen(add):
	#os.system('ssh %s  yum install hadoop -y'%add)
	#os.system('ssh %s  yum install jdk -y'%add)
	#CORE-SITE.XML
	f=open("/root/Desktop/core-site.xml",'w')
	f.writelines('''<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>	
<!-- Put site-specific property overrides in this file. -->	
<configuration>
<property>
<name>fs.default.name</name>
<value>hdfs://%s:9001</value>
</property>
</configuration>'''%add)
	f.close()
	#HDFS-SITE.XML
	f=open("/root/Desktop/hdfs-site.xml",'w')
	f.writelines('''<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>	
<!-- Put site-specific property overrides in this file. -->	
<configuration>
<property>
<name>dfs.name.dir</name>
<value>/hdfsdata</value>
</property>
</configuration>''')
	f.close()

	x=os.system('scp /root/Desktop/{core-site.xml,hdfs-site.xml} root@%s:/etc/hadoop/'%add)
	if x==0:
		print"files successfully created"
	
#namenode format
		os.system('ssh %s hadoop namenode -format'%add)
		os.system('ssh %s hadoop-daemon.sh start namenode'%add)
		print "namenode created on ip  %s"%add
		
		print "namenode created check using jps command"
	
	else:
		print "error creating files on namenode"







#####JOBTRACKER FUNCTION
def jobt(job,add):
	#os.system('ssh %s  yum install hadoop -y'%job)
	#os.system('ssh %s  yum install jdk -y'%job)
#mapred-site
	f=open("/root/mapred-site.xml",'w')
	f.writelines('''<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>	
<!-- Put site-specific property overrides in this file. -->	
<configuration>
<property>
<name>mapred.job.tracker</name>
<value>%s:9002</value>
</property>
</configuration>'''%job)
	f.close()
#core site	
	f=open("/root/core-site.xml",'w')
	f.writelines('''<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>	
<!-- Put site-specific property overrides in this file. -->	
<configuration>
<property>
<name>fs.default.name</name>
<value>hdfs://%s:9001</value>
</property>
</configuration>'''%add)
	f.close()
	x=os.system('scp /root/{core-site.xml,mapred-site.xml} root@%s:/etc/hadoop/'%job)
#command to start job tracker
	if x==0:
		print"files successfully created"
	
		os.system('ssh %s hadoop-daemon.sh start jobtracker'%job)
		print "job tracker created on ip %s"%job
	else:
		print "error creating files on jobtracker"





def datat(add,job,dat):     #FUNCTION DATANODE
	#os.system('ssh %s  yum install hadoop -y'%dat)
	#os.system('ssh %s  yum install jdk -y'%dat)	
#core-site
	f=open("/root/Music/core-site.xml",'w')
	f.writelines('''<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>	
<!-- Put site-specific property overrides in this file. -->	
<configuration>
<property>
<name>fs.default.name</name>
<value>hdfs://%s:9001</value>
</property>
</configuration>'''%add)
	f.close()
	#hdfs-site
	os.system('mkdir -p /datan')
	f=open("/root/Music/hdfs-site.xml",'w')
	f.writelines('''<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>	
<!-- Put site-specific property overrides in this file. -->	
<configuration>
<property>
<name>dfs.data.dir</name>
<value>/datan</value>
</property>
</configuration>''')
	f.close()
#MAPRED -SITE
	f=open("/root/Music/mapred-site.xml",'w')
	f.writelines('''<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>	
<!-- Put site-specific property overrides in this file. -->	
<configuration>
<property>
<name>mapred.job.tracker</name>
<value>%s:9002</value>
</property>
</configuration>'''%job)
	f.close()
	x=os.system('scp /root/Music/{core-site.xml,mapred-site.xml,hdfs-site.xml} root@%s:/etc/hadoop/'%dat)
	if x==0:

#START DATANODE
		os.system('ssh %s hadoop-daemon.sh start datanode'%dat)
#START TASKTRACKER
		os.system('ssh %s hadoop-daemon.sh start tasktracker'%dat)	
		print "datanode and task tracker created on ip  %s"%dat
	else:
		print "error creating files on datanode"

#####MAKE CLIENT



def clien(add,name):
	#os.system('ssh %s  yum install hadoop -y'%add)
	#os.system('ssh %s  yum install jdk -y'%add)
	#CORE-SITE.XML
	f=open("/root/Desktop/core-site.xml",'w')
	f.writelines(''' <?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>	
<!-- Put site-specific property overrides in this file. -->	
<configuration>
<property>
<name>fs.default.name</name>
<value>hdfs://%s:9001</value>
</property>
</configuration>'''%name)
	f.close()


#def decomission():
	
	





us=raw_input('enter username\n')
		
passw=getpass.getpass('enter password')
		
if us== 'root' and passw == 'q'  :
	while True :


		print '''1. INSTALL HADOOP
		2. INSTALL JDK
		3. TYPICAL INSTALL: CREATE "DISTRIBUTED" CLUSTER AUTOMATICALLY
		4. CUSTOM INSTALLATION
		5. EXIT '''

		ch=raw_input('enter your choice:')

		if int(ch)==1:
			os.system('yum install hadoop -y')
		elif int(ch)==2:
			os.system('yum install jdk -y')
		elif int(ch)==3:
		#find netid of current computer
			net=commands.getoutput("ifconfig eth1 | grep 192 | awk '{print$2}'|cut -c 6-20")
			net=net+'/24'
			netip=commands.getoutput("nmap -sP %s | grep 192 | awk '{print$5}'"%net)
			netiplist=netip.split('\n')
		#	if len(netiplist)<4:
		#		print "insufficient number of active nodes available"
		#		break #### do not install if number of nodes is less tha n 4
	
		#find total ram memory of each active ip in the network
			memo=dict() #dictionary containing memory info of all active ips
			for item in netiplist: 
				x=commands.getoutput("ssh %s  free -m | grep Mem | awk '{print$2}'"%item)
				memo[item]=x 
				print memo
			maxi=0
		#taking out ip with max ram for namenode
			for v in memo.itervalues():
				if v>maxi:
					maxi=v


			key=0
			for m,r in memo.iteritems():
				if maxi==r:
					key=m

			memo.pop(key)
		#   """"""key"""""""' is the ip with highest RAM

			print "ip of namenode is %s"%key
			print "CREATING NAMENODE PLEASE WAIT...................."
			namen(key) #CALL TO NAMENODE FUNCTION WHICH WILL CREATE NAMENODE ON THE GIVEN IP
		
		# taking out ip with second highest ram for "job tracker"
			smax=0
			for v in memo.itervalues():
				if v>smax:
					smax=v


			keyt=0
			for m,r in memo.iteritems():
				if smax==r:
					keyt=m

			memo.pop(keyt)
		# """""""""""keyt """""""""""""is ip with second highest ram for jobtracker
			print "ip of job tracker is %s"%keyt
			print "creating job tracker Please wait....................." 



			thread.start_new_thread(jobt,(keyt,key)) #CALL TO JOBTRACKER FUNCTION WHICH WILL CREATE JOBTRACKER ON THE GIVEN IP
		###########################################3
			dl=list() #datanode list
			for d in memo.iterkeys():
				dl.append(d)

	
		# """""dl"""" is the list of all ip of nodes which will be data nodes and task trackers
			print "list of ip's which will be data nodes and task trackers:"
			print dl
		# creating data and task trackers on designated ip


			for item in dl:
				thread.start_new_thread(datat,(key,keyt,item)) #call to datanode function

	
			print "CLUSTER CREATED SUCCESSFULLY"
	
			print "PLEASE RUN JPS COMMAND ON EACH ACTIVE NODE TO KNOW WHICH DAEMON SERVICE IT RUNS"


			print "ENTER URL %s:50070 IN WEB BROWSER ON ANY NODE TO ACCESS WEB INTERFACE OF NAMENODE"%key
			print "ENTER URL %s:50030 IN WEB BROWSER ON ANY NODE TO ACCESS WEB INTERFACE OF JOBTRACKER"%keyt
		

		elif int(ch)==4:
			#find netid of current computer
			net=commands.getoutput("ifconfig eth0 | grep 192 | awk '{print$2}'| cut -c 6-20")
			net=net+'/24'
			print net
			netip=commands.getoutput("nmap -sP %s | grep 192 | awk '{print$5}'"%net)
			netiplist=netip.split('\n')
			print "available IP's in your network are:"
			it=1
			for item in netiplist:
				print "%d - %s"%(it,item)
				it=it+1
	
			sel=raw_input("select an IP:")
			wtd=raw_input('''select what to do with the ip:
		1. create namenode
		2. create job tracker
		3. create datanode and jobtracker
		4. create client
		5. find current running service on node
		6. exit''')
			if int(wtd)==1:
				namen(sel)
				print("namenode created")		
		

			elif int(wtd)==2:
				nam=raw_input("give IP of name node: ")
				jobt(sel,nam)
				print("jobtracker  created")


			elif int(wtd)==3:
				nam=raw_input("give IP of name node: ")
				job=raw_input("give IP of jobtracker node: ")
				datat(nam,job,sel)
				print("data node and task tracker created")


	
			elif int(wtd)==4:
				nam=raw_input("enter IP of namenode")
				clien(sel,nam)
				print("client created")

			elif int(wtd)==5:
				jp=commands.getouput("ssh %s  /usr/java/jdk1.7.0_51/bin/jps"%sel)
				print jp

	
			#elif wtd==6:
			#	name=raw_input("enter IP of namenode: ")
			#	job=raw_input("enter IP of jobtracker: ")
			#	decomission(name,job)
		



			else:
				print("wrong choice")




		elif int(ch)==5:
			exit()
		else:
			print "wrong choice"			



