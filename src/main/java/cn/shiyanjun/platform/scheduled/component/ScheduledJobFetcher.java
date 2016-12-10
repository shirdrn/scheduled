package cn.shiyanjun.platform.scheduled.component;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.squareup.okhttp.MediaType;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.RequestBody;
import com.squareup.okhttp.Response;

import cn.shiyanjun.platform.api.LifecycleAware;
import cn.shiyanjun.platform.api.common.AbstractComponent;
import cn.shiyanjun.platform.api.constants.JobStatus;
import cn.shiyanjun.platform.api.utils.NamedThreadFactory;
import cn.shiyanjun.platform.scheduled.common.JobPersistenceService;
import cn.shiyanjun.platform.scheduled.common.Protocol;
import cn.shiyanjun.platform.scheduled.common.ResourceManagementProtocol;
import cn.shiyanjun.platform.scheduled.constants.ConfigKeys;
import cn.shiyanjun.platform.scheduled.dao.entities.Job;

/**
 * For a given <code>fetchJobInterval</code>, this component is able to periodically
 * fetch jobs with status {@link JobStatus#SUBMITTED} from job database.
 * 
 * @author yanjun
 */
public class ScheduledJobFetcher extends AbstractComponent implements LifecycleAware {

	private static final Log LOG = LogFactory.getLog(ScheduledJobFetcher.class);
	private static final int INITIAL_DELAY_TIME = 5000;
	private final ResourceManagementProtocol protocol;
	private ScheduledExecutorService fetchJobPool;
	private final int fetchJobInterval;
	private final JobPersistenceService jobPersistenceService;
	private final Protocol<JobStatus, List<Job>> jobSelector = new SubmittedJobSelector();
	private final Protocol<RESTRequest, JSONObject> jobOrchestrater = new RESTJobOrchestrater();
	
	public ScheduledJobFetcher(ResourceManagementProtocol protocol) {
		super(protocol.getContext());
		this.protocol = protocol;
		fetchJobInterval = context.getInt(ConfigKeys.SCHEDULED_FETCH_JOB_INTERVAL_MILLIS, 3000);
		LOG.info("Configs: fetchJobInterval=" + fetchJobInterval + ", initialDelay=" + INITIAL_DELAY_TIME);
		
		jobPersistenceService = protocol.getJobPersistenceService();
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
				fetch();
			} catch (Exception e) {
				LOG.warn("Error occured when fetching submitted jobs: ", e);
			}
		}

		private void fetch() throws Exception {
			// select submitted jobs from database
			JobStatus fromStatus = JobStatus.SUBMITTED;
			List<Job> submittedJobs = jobSelector.request(fromStatus);
			LOG.debug("Fetched jobs: " + submittedJobs);
			if(submittedJobs.size() > 0) {
				LOG.info("Fetch jobs: count=" + submittedJobs.size());
			}
			
			for(Job job : submittedJobs) {
				Integer jobId = null;
				try{
					jobId = job.getId();
					JobStatus toStatus = JobStatus.FETCHED;
					job.setStatus(toStatus.getCode()); 
					jobPersistenceService.updateJobByID(job);
					LOG.info("Job fetched: jobId=" + jobId + ", fromStatus=" + fromStatus + ", toStatus=" + toStatus);
					LOG.info("Job info: id=" + jobId + ", params=" + job.getParams());
					
					int jobType = job.getJobType();
					String jsonParams = job.getParams();
					JSONObject jobData = jobOrchestrater.request(new RESTRequest(jsonParams, jobId, jobType));
					
					if(!jobData.isEmpty()) {
						// prepare to execute queueing
						protocol.getQueueingManager().dispatch(jobData);
					}
				} catch(Exception e) {
					LOG.error("Fail to build: jobId=" + job.getId() + ", params=" + job.getParams(), e);
				}
			}
		}
	}
	
	private class SubmittedJobSelector implements Protocol<JobStatus, List<Job>> {

		@Override
		public List<Job> request(JobStatus in) {
			List<Job> submittedJobs = Lists.newArrayList();
			try {
				submittedJobs = jobPersistenceService.getJobByState(in);
			} catch (Exception e) {
				LOG.warn("Failed to fetch job: ", e);
			}
			return submittedJobs;
		}
		
	}
	
	private class RESTJobOrchestrater implements Protocol<RESTRequest, JSONObject> {

		private final String jobOrchestrateServiceUrl;
		private final OkHttpClient client = new OkHttpClient();
		private final MediaType mediaType = MediaType.parse("application/json");
		
		public RESTJobOrchestrater() {
			jobOrchestrateServiceUrl = context.get(ConfigKeys.JOB_ORCHESTRATE_SERVICE);
			Preconditions.checkArgument(jobOrchestrateServiceUrl != null, "Job Orchestrate Service url shouldn't be null!");
			LOG.info("Configs: jobOrchestrateServiceUrl=" + jobOrchestrateServiceUrl);
		}
		
		@Override
		public JSONObject request(RESTRequest in) {
			JSONObject jobData = new JSONObject();
			// build a job from known JSON parameters
			Request request = buildHttpRequest(in.jsonParams, in.jobType, in.jobId);
			LOG.info("Prepare to request job orchestrate service: " + request);
			try {
				Response response = client.newCall(request).execute();
				if(response != null && response.isSuccessful()) {
					String result = response.body().string();
					LOG.info("Responsed orchestrated job: " + result);
					jobData = JSONObject.parseObject(result);
					// release all system resources
					response.body().close();
				} else {
					LOG.warn("Fail to parse & orchestrate job: jobId=" + in.jobId + ", jobType=" + in.jobType + ", params=" + in.jsonParams);
				}
			} catch (IOException e) {
				LOG.warn("Failed to orchestrate job: ", e);
			}
			return jobData;
		}
		
		private Request buildHttpRequest(String jobParameters, int jobType, int jobId) {
			String url = jobOrchestrateServiceUrl + "/" + jobType + "/" + jobId;
			RequestBody body = RequestBody.create(mediaType, jobParameters);
			Request request = new Request.Builder()
					.url(url)
					.post(body)
					.addHeader("content-type", "application/json")
					.addHeader("cache-control", "no-cache")
					.build();
			return request;
		}
	}
	
	
	class RESTRequest {
		
		String jsonParams;
		int jobId;
		int jobType;
		
		private RESTRequest(String jsonParams, int jobId, int jobType) {
			super();
			this.jsonParams = jsonParams;
			this.jobId = jobId;
			this.jobType = jobType;
		}
	}

}
