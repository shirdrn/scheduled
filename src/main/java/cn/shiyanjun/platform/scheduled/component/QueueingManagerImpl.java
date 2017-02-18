package cn.shiyanjun.platform.scheduled.component;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;

import cn.shiyanjun.platform.api.common.AbstractComponent;
import cn.shiyanjun.platform.api.constants.JobStatus;
import cn.shiyanjun.platform.api.constants.TaskStatus;
import cn.shiyanjun.platform.api.utils.Time;
import cn.shiyanjun.platform.scheduled.api.ComponentManager;
import cn.shiyanjun.platform.scheduled.api.JobPersistenceService;
import cn.shiyanjun.platform.scheduled.api.JobQueueingService;
import cn.shiyanjun.platform.scheduled.api.QueueingManager;
import cn.shiyanjun.platform.scheduled.api.TaskPersistenceService;
import cn.shiyanjun.platform.scheduled.constants.ScheduledConstants;
import cn.shiyanjun.platform.scheduled.dao.entities.Job;
import cn.shiyanjun.platform.scheduled.dao.entities.Task;

public class QueueingManagerImpl extends AbstractComponent implements QueueingManager {

	private static final Log LOG = LogFactory.getLog(QueueingManagerImpl.class);
	private final ComponentManager manager;
	private final BlockingQueue<JSONObject> queueingQueue = Queues.newLinkedBlockingQueue();
	private volatile boolean running = true;
	private final Thread queueingWorker;
	private final Map<String, QueueingContext> queueingContexts = Maps.newHashMap();
	private final Map<Integer, String> jobTypeToQueueNames = Maps.newHashMap();
	private final JobPersistenceService jobPersistenceService;
	private final TaskPersistenceService taskPersistenceService;
	private final Set<String> queueNameSet = Sets.newHashSet();
	
    public QueueingManagerImpl(ComponentManager cm) {
		super(cm.getContext());
		this.manager = cm;
		queueingWorker = new QueueingWorker();
		queueingWorker.setName("QUEUEING-WORKER");
		jobPersistenceService = manager.getJobPersistenceService();
		taskPersistenceService = manager.getTaskPersistenceService();
	}
    
    @Override
	public void start() {
    	queueingWorker.start();		
	}

	@Override
	public void stop() {
		running = false;		
	}
    
	@Override
	public void collect(JSONObject job) {
		if(job != null) {
			queueingQueue.add(job);
		}
	}
	
	@Override
	public void registerQueue(String queueName, int... types) {
		if(!queueNameSet.contains(queueName)) {
			final JobQueueingService jobQueueingService = 
					new RedisJobQueueingService(context, queueName, manager.getJedisPool());
			QueueingContext queueingContext = new QueueingContext(queueName);
			queueingContext.jobQueueingService = jobQueueingService;
			for(int type : types) {
				queueingContext.jobTypes.add(type);
				jobTypeToQueueNames.put(type, queueName);
			}
			queueingContexts.put(queueName, queueingContext);
			queueNameSet.add(queueName);
		} else {
			LOG.warn("Queue already registered: queueName=" + queueName);
		}
	}
	
	public static class QueueingContext {
		
		private final String queueName;
		private final Set<Integer> jobTypes = Sets.newHashSet();
		private JobQueueingService jobQueueingService;
		
		public QueueingContext(String queue) {
			super();
			this.queueName = queue;
		}

		public JobQueueingService getJobQueueingService() {
			return jobQueueingService;
		}

		public Set<Integer> getJobTypes() {
			return jobTypes;
		}

		public String getQueueName() {
			return queueName;
		}
		
	}
	
	private final class QueueingWorker extends Thread {
		
		@Override
		public void run() {
			while(running) {
				JSONObject job = null;
				try {
					// control to take a job from the queue
					if(manager.isSchedulingOpened()) {
						job = queueingQueue.take();
						if(job != null) {
							int jobId = job.getIntValue(ScheduledConstants.JOB_ID);
							int jobType = job.getIntValue(ScheduledConstants.JOB_TYPE);
							
							List<JSONObject> jsonTasks = extractTasks(job);
							
							List<Task> userTasks = Lists.newArrayList();
							jsonTasks.forEach(jTask -> createAndCollectTask(jobId, userTasks, jTask));
							
							try {
								// insert task informations into database, with initial status: CREATED
								taskPersistenceService.insertTasks(userTasks);
								
								// add tasks of a job to waiting queue
								doQueueing(jobId, jobType, userTasks);
							} catch (Exception e) {
								LOG.warn("Failed to save or queueing job: " + job, e);
								// update job status to FAILED
								Job myJob = new Job();
								myJob.setId(jobId);
								myJob.setStatus(JobStatus.FAILED.getCode());
								myJob.setDoneTime(new Timestamp(Time.now()));
								jobPersistenceService.updateJobByID(myJob);
							}
						}
					} else {
						Thread.sleep(3000);
					}
				} catch (Exception e) {
					LOG.error("Failed to queueing: job=" + job, e);
				}
			}
		}

		private void createAndCollectTask(int jobId, List<Task> userTasks, JSONObject jTask) {
			int taskType = jTask.getIntValue(ScheduledConstants.TASK_TYPE);
			int seqNo = jTask.getIntValue(ScheduledConstants.SEQ_NO);
			String parsedExpression = jTask.getString(ScheduledConstants.PARSED_EXPRESSION);
			Task task = new Task();
			task.setTaskType(taskType);
			task.setJobId(jobId);
			task.setSeqNo(seqNo);
			task.setParams(parsedExpression);
			task.setStatus(TaskStatus.CREATED.getCode());
			userTasks.add(task);
		}
		
		List<JSONObject> extractTasks(JSONObject job) {
			JSONArray stages = job.getJSONArray(ScheduledConstants.STAGES);
			List<JSONObject> jsonTasks = Lists.newArrayList();
			
			for (int i = 0; i < stages.size(); i++) {
				JSONObject stage = stages.getJSONObject(i);
				JSONArray tasks = stage.getJSONArray(ScheduledConstants.TASKS);
				for (int j = 0; j < tasks.size(); j++) {
					jsonTasks.add(tasks.getJSONObject(j));
				}
			}
			return jsonTasks;
		}

		private void doQueueing(int jobId, int jobType, List<Task> tasks) {
			JSONObject detail = new JSONObject(true);
			// first time queued job: jobStatus=QUEUEING, taskStatus=WAIT_TO_BE_SCHEDULED
			detail.put(ScheduledConstants.JOB_ID, jobId);
			detail.put(ScheduledConstants.JOB_STATUS, JobStatus.QUEUEING.toString());
			detail.put(ScheduledConstants.TASK_COUNT, tasks.size());
			detail.put(ScheduledConstants.TASK_ID, -1);
			detail.put(ScheduledConstants.SEQ_NO, -1);
			detail.put(ScheduledConstants.TASK_STATUS, TaskStatus.CREATED.toString());
			detail.put(ScheduledConstants.LAST_UPDATE_TS, Time.now());
			
			final JobQueueingService qs = getJobQueueingService(jobType);
			// check to ensure the correction of data,
			// just not permit to get  the duplicated job ID in Redis storage
			qs.getJobs().forEach(data -> {
				JSONObject o = JSONObject.parseObject(data);
				int jobIdInRedis = o.getIntValue(ScheduledConstants.JOB_ID);
				Preconditions.checkArgument(jobIdInRedis == jobId, "Inconsistent data occured in Redis storage!");
			});
			qs.enqueue(jobId, detail.toJSONString());
			
			LOG.info("Job queued: " + detail);
			updateJobAndTasks(jobId, tasks);
		}

		private void updateJobAndTasks(int jobId, List<Task> tasks) {
			// update job status to QUEUEING
			Job job = new Job();
			job.setId(jobId);
			job.setStatus(JobStatus.QUEUEING.getCode());
			job.setDoneTime(new Timestamp(Time.now()));
			jobPersistenceService.updateJobByID(job);

			// update all task  status to QUEUEING for job
			tasks.forEach(task -> {
				task.setStatus(TaskStatus.QUEUEING.getCode());
				taskPersistenceService.updateTaskByID(task);
			});
		}
	}
	
	private JobQueueingService getJobQueueingService(int jobType) {
		String selectedQueueName = jobTypeToQueueNames.get(jobType);
		return queueingContexts.get(selectedQueueName).getJobQueueingService();
	}

	@Override
	public QueueingContext getQueueingContext(String queueName) {
		return queueingContexts.get(queueName);
	}

	@Override
	public Set<String> queueNames() {
		return queueNameSet;
	}

	@Override
	public String getQueueName(int jobType) {
		return jobTypeToQueueNames.get(jobType);
	}

}
