package cn.shiyanjun.platform.scheduled.component;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Preconditions;

import cn.shiyanjun.platform.api.constants.JobStatus;
import cn.shiyanjun.platform.api.utils.NamedThreadFactory;
import cn.shiyanjun.platform.api.utils.Pair;
import cn.shiyanjun.platform.api.utils.Time;
import cn.shiyanjun.platform.scheduled.api.ComponentManager;
import cn.shiyanjun.platform.scheduled.api.JobFetcher;
import cn.shiyanjun.platform.scheduled.api.JobPersistenceService;
import cn.shiyanjun.platform.scheduled.api.Protocol;
import cn.shiyanjun.platform.scheduled.common.AbstractJobController;
import cn.shiyanjun.platform.scheduled.constants.ConfigKeys;
import cn.shiyanjun.platform.scheduled.dao.entities.Job;
import cn.shiyanjun.platform.scheduled.protocols.JobFetchProtocolManager;
import cn.shiyanjun.platform.scheduled.protocols.JobOrchestrationProtocolManager;
import cn.shiyanjun.platform.scheduled.protocols.JobOrchestrationProtocolManager.RESTRequest;

/**
 * For a given <code>fetchJobInterval</code>, this component is able to periodically
 * fetch jobs with status {@link JobStatus#SUBMITTED} from job database.
 * 
 * @author yanjun
 */
public class ScheduledJobFetcher extends AbstractJobController implements JobFetcher {

	private static final Log LOG = LogFactory.getLog(ScheduledJobFetcher.class);
	private static final int INITIAL_DELAY_TIME = 5000;
	private final ComponentManager manager;
	private ScheduledExecutorService fetchJobPool;
	private final int fetchJobInterval;
	private final JobPersistenceService jobPersistenceService;
	private final JobFetchProtocolManager jobFetchProtocolManager;
	private final JobOrchestrationProtocolManager jobOrchestrationProtocolManager;
	private final Enum<?> jobOrchestrationProtocol;
	private final Enum<?> jobFetchProtocol;
	private volatile boolean isSchedulingOpened = true;
	private volatile String maintenanceSegmentStartTime;
	private volatile String maintenanceSegmentEndTime;
	
	public ScheduledJobFetcher(ComponentManager manager) {
		super(manager.getContext());
		this.manager = manager;
		fetchJobInterval = context.getInt(ConfigKeys.SCHEDULED_FETCH_JOB_INTERVAL_MILLIS, 3000);
		LOG.info("Configs: fetchJobInterval=" + fetchJobInterval + ", initialDelay=" + INITIAL_DELAY_TIME);
		
		maintenanceSegmentStartTime = context.get(ConfigKeys.SCHEDULED_MAINTENANCE_TIME_SEGMENT_START, "03:00:00");
		maintenanceSegmentEndTime = context.get(ConfigKeys.SCHEDULED_MAINTENANCE_TIME_SEGMENT_END, "03:30:00");
		LOG.info("Configs: maintenanceSegmentStartTime=" + maintenanceSegmentStartTime + ", maintenanceSegmentEndTime=" + maintenanceSegmentEndTime);
		
		jobPersistenceService = manager.getJobPersistenceService();
		
		jobOrchestrationProtocolManager = new JobOrchestrationProtocolManager(context);
		jobOrchestrationProtocolManager.initialize();
		String jobOrchestrationStringProtocol = context.get(ConfigKeys.SERVICE_JOB_ORCHESTRATE_PROTOCOL);
		Preconditions.checkArgument(jobOrchestrationStringProtocol != null);
		jobOrchestrationProtocol = jobOrchestrationProtocolManager.ofType(jobOrchestrationStringProtocol);
		LOG.info("Protocol: jobOrchestrationProtocol=" + jobOrchestrationProtocol);
		
		jobFetchProtocolManager = new JobFetchProtocolManager(manager, context);
		jobFetchProtocolManager.initialize();
		String jobFetchStringProtocol = context.get(ConfigKeys.SERVICE_JOB_FETCH_PROTOCOL);
		Preconditions.checkArgument(jobFetchStringProtocol != null);
		jobFetchProtocol = jobFetchProtocolManager.ofType(jobFetchStringProtocol);
		LOG.info("Protocol: jobFetchProtocol=" + jobFetchProtocol);
	}

	@Override
	public void start() {
		fetchJobPool = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("JOB-FETCHER"));
		fetchJobPool.scheduleAtFixedRate(
				new FetchJobThread(), INITIAL_DELAY_TIME, fetchJobInterval, TimeUnit.MILLISECONDS);
		LOG.info("Fetch job pool started: " + fetchJobPool);
	}

	@Override
	public void stop() {
		fetchJobPool.shutdown();
	}
	
	@Override
	public boolean isSchedulingOpened() {
		return isSchedulingOpened;
	}

	@Override
	public void setSchedulingOpened(boolean isSchedulingOpened) {
		this.isSchedulingOpened = isSchedulingOpened;
	}
	
	@Override
	public Pair<String, String> getMaintenanceTimeSegment() {
		return new Pair<>(maintenanceSegmentStartTime, maintenanceSegmentEndTime);
	}

	@Override
	public void updateMaintenanceTimeSegment(String startTime, String endTime) {
		maintenanceSegmentStartTime = startTime;
		maintenanceSegmentEndTime = endTime;
		LOG.info("Maintenance time segment updated: startTime=" + startTime + ", endTime=" + endTime);
	}
	
	/**
	 * Read jobs from job database, and build jobs from the given JSON parameters. Finally
	 * the built jobs will be dispatched to the job queueing manager to be queued.
	 * 
	 * @author yanjun
	 */
	final class FetchJobThread implements Runnable {
		
		public FetchJobThread() {
			super();
		}
		
		@Override
		public void run() {
			try {
				if(shouldTryToFetch()) {
					fetch();
				}
			} catch (Exception e) {
				LOG.warn("Error occured when fetching submitted jobs: ", e);
			}
		}
		
		private boolean shouldTryToFetch() {
			if(isSchedulingOpened) {
				String start = maintenanceSegmentStartTime.replaceAll(":", "");
				String end = maintenanceSegmentEndTime.replaceAll(":", "");
				String current = Time.formatCurrentHourTime().replaceAll(":", "");
				int currentTime = Integer.parseInt(current);
				if(currentTime < Integer.parseInt(start) || currentTime > Integer.parseInt(end)) {
					return true;
				}
			}
			return false;
		}

		private void fetch() throws Exception {
			// select submitted jobs from database
			JobStatus fromStatus = JobStatus.SUBMITTED;
			List<Job> submittedJobs = jobFetchProtocolManager.select(jobFetchProtocol).request(fromStatus);
			LOG.debug("Fetched jobs: " + submittedJobs);
			if(submittedJobs.size() > 0) {
				LOG.info("Fetch jobs: count=" + submittedJobs.size());
			}
			
			submittedJobs.forEach(job -> {
				Integer jobId = null;
				try{
					jobId = job.getId();
					if(shouldCancelJob(jobId)) {
						jobCancelled(jobId, () -> {
							job.setStatus(JobStatus.CANCELLED.getCode()); 
							jobPersistenceService.updateJobByID(job);
						});
					} else {
						JobStatus toStatus = JobStatus.FETCHED;
						job.setStatus(toStatus.getCode()); 
						jobPersistenceService.updateJobByID(job);
						LOG.info("Job fetched: jobId=" + jobId + ", fromStatus=" + fromStatus + ", toStatus=" + toStatus);
						LOG.info("Job info: id=" + jobId + ", params=" + job.getParams());
						
						int jobType = job.getJobType();
						String jsonParams = job.getParams();
						Protocol<RESTRequest, JSONObject> p = jobOrchestrationProtocolManager.select(jobOrchestrationProtocol);
						JSONObject jobData = p.request(new RESTRequest(jsonParams, jobId, jobType));
						
						if(!jobData.isEmpty()) {
							// prepare to execute queueing
							manager.getQueueingManager().collect(jobData);
						}
					}
				} catch(Exception e) {
					LOG.error("Fail to build: jobId=" + job.getId() + ", params=" + job.getParams(), e);
				}
			});
		}
	}

}
