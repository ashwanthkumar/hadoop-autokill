Hadoop AutoKill
---------------
Hadoop AutoKill is small hack which helps you have control over the Hadoop Jobs that you run on your Hadoop Cluster. When you have certain restrictions on the amount of time you have run a Job, then this piece of JAR will help you. 

You just have the change the values of the `JOB_TRACKER_IP`, `JOB_TRACKER_PORT`, `HADOOP_CONF` and `JOB_TIMEOUT` parameters in the `MonitorAllJobs` class. The last parameter can also be changed while executing also. 

You need to execute the JAR as

`$ hadoop jar HadoopAutoKill.jar AutoKill 3600000`

PS: This is an eclipse project, and you might want to replace the User library Hadoop-0.20.205.0 with actual Hadoop-0.20.205.0 jars (from lib folder, inc. hadoop-core*.jar)

Word of Caution
---------------
I wanted to learn how to use the `JobClient` in Hadoop, for which I created this small hack. This piece of code has no JUnit tests, but was tested on a Single node cluster and it just works. 