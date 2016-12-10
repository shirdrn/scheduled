package cn.shiyanjun.platform.scheduled.component;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.fastjson.JSONObject;

import cn.shiyanjun.platform.api.common.AbstractComponent;
import cn.shiyanjun.platform.api.constants.JobStatus;
import cn.shiyanjun.platform.api.constants.TaskStatus;
import cn.shiyanjun.platform.api.constants.TaskType;
import cn.shiyanjun.platform.api.utils.Time;
import cn.shiyanjun.platform.scheduled.common.JobQueueingService;
import cn.shiyanjun.platform.scheduled.common.ResourceManagementProtocol;
import cn.shiyanjun.platform.scheduled.common.ResourceMetadataManager;
import cn.shiyanjun.platform.scheduled.common.ScheduledTask;
import cn.shiyanjun.platform.scheduled.common.SchedulingStrategy;
import cn.shiyanjun.platform.scheduled.common.TaskPersistenceService;
import cn.shiyanjun.platform.scheduled.constants.ScheduledConstants;
import cn.shiyanjun.platform.scheduled.dao.entities.Task;

public class MaxConcurrenySchedulingStrategy extends AbstractComponent implements SchedulingStrategy {

	private static final Log LOG = LogFactory.getLog(MaxConcurrenySchedulingStrategy.class);
	private final ResourceMetadataManager resourceMetadataManager;
	private final TaskPersistenceService taskPersistenceService;
	private final ResourceManagementProtocol protocol;

	public MaxConcurrenySchedulingStrategy(ResourceManagementProtocol protocol) {
		super(protocol.getContext());
		this.protocol = protocol;
        this.taskPersistenceService = protocol.getTaskPersistenceService();
        this.resourceMetadataManager = protocol.getResourceMetadataManager();
	}

	@Override
    public synchronized Optional<ScheduledTask> offerTask(String queue, TaskType taskType) {
		Optional<ScheduledTask> got = Optional.empty();
		// allocate resource
		int availableResources = resourceMetadataManager.queryResource(queue, taskType);
		LOG.debug("Available resources: queue=" + queue + ", taskType=" + taskType + ", resources=" + availableResources);
        if (availableResources > 0) {
            DefaultQueueingManager.QueueingContext queueingContext = protocol.getQueueingManager().getQueueingContext(queue);
            JobQueueingService qs = queueingContext.getJobQueueingService();
            Set<String> jobs = qs.getJobs();
            if(CollectionUtils.isNotEmpty(jobs)) {
                
                // (job, task) with statuses 
            	// (QUEUEING, WAIT_TO_BE_SCHEDULED) or (QUEUEING, SUCCEED)
                // in Redis queue should be scheduled
                for(String job : jobs) {
                    JSONObject jsonObject = JSONObject.parseObject(job);
                    int jobId = jsonObject.getIntValue(ScheduledConstants.JOB_ID);
                    String jobStatus = jsonObject.getString(ScheduledConstants.JOB_STATUS);
                    String taskStatus = jsonObject.getString(ScheduledConstants.TASK_STATUS);
                    int serialNo = jsonObject.getIntValue(ScheduledConstants.SERIAL_NO);
                    int taskCount = jsonObject.getIntValue(ScheduledConstants.TASK_COUNT);
                    
                    if(jobStatus.equals(JobStatus.QUEUEING.toString())) {
                    	// priority of a task belonging a RUNNING job is higher than new tasks of submitted jobs
                    	// (job, task) = (QUEUEING, SUCCEEDED)
                    	if(taskStatus.equals(TaskStatus.SUCCEEDED.toString())) {
                    		List<Task> tasks = taskPersistenceService.getTasksFor(jobId);
                        	if(tasks != null && !tasks.isEmpty()) {
                        		Task utask = null;
                        		for(Task t : tasks) {
                        			if(t.getSerialNo() == serialNo + 1) {
                        				utask = t;
                        				break;
                        			}
                        		}
                        		if(taskType.getCode() == utask.getTaskType()) {
                        			ScheduledTask scheduledTask = new ScheduledTask(utask);
                        			scheduledTask.setTaskCount(taskCount);
                        			updateJobInRedisAfterTaskScheduled(qs, utask, jsonObject, taskStatus);
                        			got = Optional.of(scheduledTask);
                        		}
                        	}
                    	} else if(taskStatus.equals(ScheduledConstants.TASK_INITIAL_STATUS.toString())) {
                    		// (job, task) = (QUEUEING, WAIT_TO_BE_SCHEDULED)
                    		List<Task> tasksBelongingJob = taskPersistenceService.getTasksFor(jobId);
                        	if(tasksBelongingJob != null && !tasksBelongingJob.isEmpty()) {
                        		Task ut = tasksBelongingJob.get(0);
                        		if(taskType.getCode() == ut.getTaskType()) {
    	                    		ScheduledTask scheduledTask = new ScheduledTask(ut);
    	                			scheduledTask.setTaskCount(taskCount);
    	                			updateJobInRedisAfterTaskScheduled(qs, ut, jsonObject, taskStatus);
    	                			got = Optional.of(scheduledTask);
                        		}
                        	}
                    	}
                    }
                    
                    
                    // allocate resource
                    if(got.isPresent()) {
                    	resourceMetadataManager.allocateResource(queue, taskType);
                    	resourceMetadataManager.currentResourceStatuses();
                    	break;
                    }
                }
            }
        }
        return got;
    }
	
    private void updateJobInRedisAfterTaskScheduled(JobQueueingService qs, Task ut, JSONObject job, String oldTaskStatus) {
    	// (job, task) = (SCHEDULED, SCHEDULED) 
    	job.put(ScheduledConstants.JOB_STATUS, JobStatus.SCHEDULED.toString());
    	job.put(ScheduledConstants.TASK_ID, ut.getId());
    	job.put(ScheduledConstants.SERIAL_NO, ut.getSerialNo());
    	job.put(ScheduledConstants.TASK_STATUS, TaskStatus.SCHEDULED.toString());
    	job.put(ScheduledConstants.LAST_UPDATE_TS, Time.now());
    	updateRedisState(ut.getJobId(), ut.getId(), ut.getSerialNo(), qs, job, oldTaskStatus);
    	
    	LOG.info("Scheduling task selected: " + job);
	}
    
    private void updateRedisState(int jobId, int taskId, int serialNo, final JobQueueingService qs, final JSONObject job, String oldTaskStatus) {
		qs.updateQueuedJob(jobId, job);
		LOG.info("Redis state changed: queue=" + qs.getQueueName() + ", job=" + job);
	}

}
