#!/bin/sh_

#RDMA
bash /proj/nova-PG0/yichen/bootstrap_scripts/local/setup-apt-ssh.sh 2
bash /proj/nova-PG0/yichen/bootstrap_scripts/remote/init.sh 2
for i in {0..1}; do ssh node-$i "ibv_devinfo"; done

#Disni
git clone https://github.com/zrlio/disni.git
cd libdisni
 ./autoprepare.sh
 ./configure --with-jdk=$JAVA_HOME
 sudo make install



#############################################
mvn clean package -Pdist -DskipTests -Dtar

host="apt180.apt.emulab.net"

# ssh
bash /proj/nova-PG0/yichen/bootstrap_scripts/local/setup-ssh.sh
#ssh -oStrictHostKeyChecking=no Yichen@apt166.apt.emulab.net "bash /proj/nova-PG0/yichen/bootstrap_scripts/local/setup-ssh.sh"

# Download jdk-8 in each node

mkdir /proj/nova-PG0/java/

#scp ~/Desktop/550/Project/jdk-8u131-linux-x64.tar.gz Yichen@${host}:/proj/nova-PG0/

# add to ~/.bash_profile
#PATH=$PATH:$HOME/bin
#export HADOOP_HOME=/usr/local/hadoop/hadoop-2.7.3
#export JAVA_HOME=/proj/nova-PG0/java/jdk1.8.0_131
#export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$PATH
#export PATH=$JAVA_HOME/bin:$PATH
#export CLASSPATH=$JAVA_HOME/lib:.

# or, copy .bash_profile
cp /proj/nova-PG0/yichen/node-setup/.bash_profile ~/.bash_profile
# and copy .vimrc
cp /proj/nova-PG0/yichen/node-setup/.vimrc ~/.vimrc

# download hadoop
cd $HADOOP_HOME
wget https://archive.apache.org/dist/hadoop/common/hadoop-2.7.3/hadoop-2.7.3.tar.gz
sudo mv hadoop/ /usr/local/
tar -xzvf hadoop-2.7.3.tar.gz

# add slaves
vi $HADOOP_HOME/etc/hadoop/slaves
h
# $HADOOP_HOME/etc/hadoop/
# hdfs-site.xml mapred-site.xml yarn-site.xml core-site.xml
# set JAVA_HOME in $HADOOP_HOME/etc/hadoop/hadoop-env.sh
cp /proj/nova-PG0/yichen/conf/*.xml $HADOOP_HOME/etc/hadoop
cp /proj/nova-PG0/yichen/conf/hadoop-env.sh $HADOOP_HOME/etc/hadoop

# copy hadoop dire to all nodes
scp -r $HADOOP_HOME Yichen@node-1:/usr/local/hadoop

# remove slaves on the slave nodes
rm $HADOOP_HOME/etc/hadoop/slaves

# format hadoop
hadoop namenode -format

# start-all.sh

# terasort 
#hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.3.jar teragen 1000000 terasort/1000000-input
#hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.3.jar terasort terasort/1000000-input terasort/1000000-output
hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.3.jar teragen 1000000 terasort/1000000-input
hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.3.jar terasort -Dmapreduce.reduce.env=LD_LIBRARY_PATH=/usr/local/lib/:$LD_LIBRARY_PATH terasort/1000000-input terasort/1000000-output

Solution:
1) "ls: .: No such file or directory"
hdfs dfs -mkdir /user
hdfs dfs -mkdir /user/{loggedin user}
hdfs dfs -ls
