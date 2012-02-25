package in.ashwanthkumar.hacks.hadoop;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * You need to start this JAR file as
 * <p>
 * <code>
 * 	$ hadoop jar HadoopAutoKill.jar start 3600000
 * </code>
 * </p>
 * 
 * @author Ashwanth.Kumar
 * 
 */
public class MonitorAllJobs {
	private static Logger _log = LoggerFactory.getLogger(MonitorAllJobs.class);

	/**
	 * Where is the JobTracker running?
	 */
	public static String JOB_TRACKER_IP = "localhost";
	/**
	 * On which port the JobTracker running?
	 */
	public static Integer JOB_TRACKER_PORT = 54311;
	/**
	 * Path where Hadoop configuration is found
	 */
	public static String HADOOP_CONF = "/usr/local/hadoop/hadoop/conf/";
	/**
	 * Amount of time to sleep after executing the killJob()
	 */
	public static Integer SLEEP_TIME = 5000;
	/**
	 * Default number of milliseconds you allow a job to run. By default the
	 * value is 1 hour
	 */
	public static Integer JOB_TIMEOUT = 3600000;

	private JobClient jobClient;
	private Configuration conf;

	/**
	 * Duration of each Hadoop job in milliseconds
	 */
	public Long durationOfEachJob;

	/**
	 * Constructor that loads the default settings from the hadoop-core-*.jar
	 * 
	 * @throws IOException
	 */
	public MonitorAllJobs() throws IOException {
		conf = new Configuration(true);
		conf = MonitorAllJobs.addSiteDefaultsToConfiguration(conf);

		this.jobClient = new JobClient(JobTracker.getAddress(conf), conf);

		/**
		 * @TODO Need to dynamically get this value
		 */
		durationOfEachJob = (long) MonitorAllJobs.JOB_TIMEOUT;
	}

	@SuppressWarnings("unused")
	private void dumpLoadedConf() throws IOException {
		PrintWriter pw = new PrintWriter(System.out, true);
		Configuration.dumpConfiguration(conf, pw);
	}

	/**
	 * Check the file path and add site default xmls to the configuration
	 */
	public static Configuration addSiteDefaultsToConfiguration(
			Configuration conf) {
		String[] siteDefaults = { "core-site.xml", "hdfs-site.xml",
				"mapred-site.xml",
				/*
				 * This site file is generally depreceated but still created
				 * when using Whirr
				 */
				"hadoop-site.xml" };
		for (String siteDefaultXML : siteDefaults) {
			String filePath = HADOOP_CONF + siteDefaultXML;
			File siteDefaultFile = new File(filePath);

			if (siteDefaultFile.exists()) {
				conf.addResource(new Path(filePath));
			}
		}

		return conf;
	}

	/**
	 * Put all the jobs running in the monitor
	 * 
	 * @throws IOException
	 */
	public void putRunningJobsInMonitor() throws IOException {
		JobStatus[] jobs = this.jobClient.getAllJobs();

		int runningJobCount = 0;
		int killedJobCount = 0;

		// Loop through all the jobs
		for (JobStatus job : jobs) {
			// Check only for Running jobs
			if (job.getRunState() == JobStatus.RUNNING) {
				if (checkAndKillJob(job.getJobID(), job))
					killedJobCount++;
				else
					runningJobCount++;
			}
		}

		_log.info("Running Job Count - " + runningJobCount);
		_log.info("Killed Job Count - " + killedJobCount);
	}

	/**
	 * We check if the job has used more than the required number of
	 * milliseconds and kill it.
	 */
	public Boolean checkAndKillJob(JobID jobId, JobStatus job) {
		try {
			Long jobRunningPeriod = System.currentTimeMillis()
					- job.getStartTime();

			_log.info(jobId + " is running for " + jobRunningPeriod
					+ " milliseconds");

			if (jobRunningPeriod > this.durationOfEachJob) {

				RunningJob runningJob = jobClient.getJob(jobId);
				runningJob.killJob();

				// Give it some time to get up from the slumber of refreshing
				// the
				// job status
				Thread.sleep(SLEEP_TIME);

				_log.info(jobId
						+ " is "
						+ HumanReadableJobStatus
								.getHumanReadableJobStatus(runningJob
										.getJobState()));

				if (runningJob.getJobState() == JobStatus.KILLED) {
					return true;
				} else {
					_log.error(jobId + " - was not Killed.");
				}
			}
		} catch (Exception e) {
			_log.error(e.getMessage());
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * Entry point of the program
	 * 
	 * @param args
	 */
	public static void main(String args[]) {
		Long grasePeriod = (long) 0;
		try {
			MonitorAllJobs autoKillJob = new MonitorAllJobs();
			Configuration conf = new Configuration();
			String[] otherArgs = new GenericOptionsParser(conf, args)
					.getRemainingArgs();

			if (otherArgs.length > 0) {
				grasePeriod = Long.parseLong(args[1]);
				autoKillJob.durationOfEachJob = grasePeriod;
			}

			autoKillJob.putRunningJobsInMonitor();
		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
		}
	}
}