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
import cn.shiyanjun.platform.scheduled.common.GlobalResourceManager;
import cn.shiyanjun.platform.scheduled.common.ResourceManager;
import cn.shiyanjun.platform.scheduled.common.TaskOrder;
import cn.shiyanjun.platform.scheduled.common.SchedulingStrategy;
import cn.shiyanjun.platform.scheduled.common.TaskPersistenceService;
import cn.shiyanjun.platform.scheduled.constants.ScheduledConstants;
import cn.shiyanjun.platform.scheduled.dao.entities.Task;

/**
 * Offer a task to be scheduled based on the maximum concurrency limits, if the resources 
 * are available. It controls to update resource counter for the specified task type, 
 * which is managed by another component {@link ResourceManager}.
 * 
 * @see {@link ResourceMetadataManagerImpl}
 * @author yanjun
 */
public class MaxConcurrencySchedulingStrategy extends AbstractComponent implements SchedulingStrategy {

	private static final Log LOG = LogFactory.getLog(MaxConcurrencySchedulingStrategy.class);
	private final ResourceManager resourceMetadataManager;
	private final TaskPersistenceService taskPersistenceService;
	private final GlobalResourceManager manager;

	public MaxConcurrencySchedulingStrategy(GlobalResourceManager manager) {
		super(manager.getContext());
		this.manager = manager;
        this.taskPersistenceService = manager.getTaskPersistenceService();
        this.resourceMetadataManager = manager.getResourceMetadataManager();
	}

	@Override
    public synchronized Optional<TaskOrder> offerTask(String queue, TaskType taskType) {
		Optional<TaskOrder> got = Optional.empty();
		// allocate resource
		int availableResources = resourceMetadataManager.queryResource(queue, taskType);
		LOG.debug("Available resources: queue=" + queue + ", taskType=" + taskType + ", resources=" + availableResources);
        if (availableResources > 0) {
            DefaultQueueingManager.QueueingContext queueingContext = manager.getQueueingManager().getQueueingContext(queue);
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
                        			TaskOrder taskOrder = new TaskOrder(utask);
                        			taskOrder.setTaskCount(taskCount);
                        			updateJobInRedisAfterTaskScheduled(qs, utask, jsonObject, taskStatus);
                        			got = Optional.of(taskOrder);
                        		}
                        	}
                    	} else if(taskStatus.equals(ScheduledConstants.TASK_INITIAL_STATUS.toString())) {
                    		// (job, task) = (QUEUEING, WAIT_TO_BE_SCHEDULED)
                    		List<Task> tasksBelongingJob = taskPersistenceService.getTasksFor(jobId);
                        	if(tasksBelongingJob != null && !tasksBelongingJob.isEmpty()) {
                        		Task ut = tasksBelongingJob.get(0);
                        		if(taskType.getCode() == ut.getTaskType()) {
    	                    		TaskOrder taskOrder = new TaskOrder(ut);
    	                			taskOrder.setTaskCount(taskCount);
    	                			updateJobInRedisAfterTaskScheduled(qs, ut, jsonObject, taskStatus);
    	                			got = Optional.of(taskOrder);
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
