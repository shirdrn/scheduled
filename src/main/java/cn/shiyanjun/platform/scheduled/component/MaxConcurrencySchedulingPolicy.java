package cn.shiyanjun.platform.scheduled.component;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Preconditions;

import cn.shiyanjun.platform.api.constants.JobStatus;
import cn.shiyanjun.platform.api.constants.TaskStatus;
import cn.shiyanjun.platform.api.constants.TaskType;
import cn.shiyanjun.platform.api.utils.Time;
import cn.shiyanjun.platform.scheduled.api.ComponentManager;
import cn.shiyanjun.platform.scheduled.api.QueueSelector;
import cn.shiyanjun.platform.scheduled.api.ResourceManager;
import cn.shiyanjun.platform.scheduled.api.SchedulingPolicy;
import cn.shiyanjun.platform.scheduled.api.StateManager;
import cn.shiyanjun.platform.scheduled.common.TaskOrder;
import cn.shiyanjun.platform.scheduled.constants.ConfigKeys;
import cn.shiyanjun.platform.scheduled.constants.ScheduledConstants;
import cn.shiyanjun.platform.scheduled.constants.QueueSelectorType;
import cn.shiyanjun.platform.scheduled.dao.entities.Task;
import cn.shiyanjun.platform.scheduled.utils.ScheduledUtils;

/**
 * Offer a task to be scheduled based on the maximum concurrency policy, if the resources 
 * are available. It controls to update resource counter for the specified task type, 
 * which is managed by another component {@link ResourceManager}.
 * 
 * @see {@link ResourceManagerImpl}
 * @author yanjun
 */
public class MaxConcurrencySchedulingPolicy implements SchedulingPolicy {

	private static final Log LOG = LogFactory.getLog(MaxConcurrencySchedulingPolicy.class);
	private final ResourceManager resourceManager;
	private final StateManager stateManager;
	private QueueSelector queueSelector;
	
	public MaxConcurrencySchedulingPolicy(ComponentManager componentManager) {
		super();
		stateManager = componentManager.getStateManager();
        resourceManager = componentManager.getResourceManager();
        
		String type = componentManager.getContext().get(
				ConfigKeys.SCHEDULED_QUEUEING_SELECTOR_NAME,
				QueueSelectorType.RAMDOM.name());
		QueueSelectorType selectorType = QueueSelectorType.RAMDOM;
		try {
			selectorType = QueueSelectorType.valueOf(type);
		} catch (Exception e) {}
		Optional<QueueSelector> qs = ScheduledUtils.getQueueSelector(selectorType);
		if(!qs.isPresent()) {
			Preconditions.checkArgument(qs.isPresent(), "queueSelector==null");
		} else {
			queueSelector = qs.get();
			queueSelector.setResourceManager(resourceManager);
		}
	}
	
	@Override
    public synchronized Optional<TaskOrder> offerTask() {
		Optional<String> selectedQueue = queueSelector.selectQueue();
		if(selectedQueue.isPresent()) {
			String queue = selectedQueue.get();
			for(TaskType taskType : resourceManager.taskTypes(queue)) {
				// allocate resource
				int availableResources = resourceManager.availableResource(queue, taskType);
				LOG.debug("Available resources: queue=" + queue + ", taskType=" + taskType + ", resources=" + availableResources);
				if (availableResources > 0) {
					Optional<TaskOrder> order = selectNextTask(queue, taskType);
					if(order.isPresent()) {
						return order;
					} else {
						continue;
					}
				}
			}
		}
        return Optional.empty();
    }
	
	private Optional<TaskOrder> selectNextTask(String queue, TaskType taskType) {
		Optional<TaskOrder> got = Optional.empty();
		Set<JSONObject> jobs = stateManager.retrieveQueuedJobs(queue);
		// (job, task) with statuses 
        // (QUEUEING, CREATED) or (QUEUEING, SUCCEED)
        // task in queue should be scheduled
        for(JSONObject job : jobs) {
        	int jobId = job.getIntValue(ScheduledConstants.JOB_ID);
        	String jobStatus = job.getString(ScheduledConstants.JOB_STATUS);
        	String taskStatus = job.getString(ScheduledConstants.TASK_STATUS);
        	int seqNo = job.getIntValue(ScheduledConstants.SEQ_NO);
        	int taskCount = job.getIntValue(ScheduledConstants.TASK_COUNT);
        	
        	JobStatus js = JobStatus.valueOf(jobStatus);
        	TaskStatus ts = TaskStatus.valueOf(taskStatus);
        	
        	try {
        		if(js == JobStatus.QUEUEING) {
        			// priority of a task belonging a RUNNING job is higher than new tasks of submitted jobs
        			// (job, task) = (QUEUEING, SUCCEEDED)
        			if(ts == TaskStatus.SUCCEEDED) {
        				List<Task> tasks = stateManager.retrieveTasks(jobId);
        				if(tasks != null && !tasks.isEmpty()) {
        					Task task = null;
        					for(Task t : tasks) {
        						if(t.getSeqNo() == seqNo + 1) {
        							task = t;
        							break;
        						}
        					}
        					if(taskType.getCode() == task.getTaskType()) {
        						TaskOrder taskOrder = new TaskOrder(queue, task);
        						taskOrder.setTaskCount(taskCount);
        						updateQueuedJobAfterTaskScheduled(queue, task, job);
        						got = Optional.of(taskOrder);
        					}
        				}
        			} else if(ts == TaskStatus.CREATED) {
        				// (job, task) = (QUEUEING, CREATED)
        				List<Task> tasksBelongingJob = stateManager.retrieveTasks(jobId);
        				if(tasksBelongingJob != null && !tasksBelongingJob.isEmpty()) {
        					Task ut = tasksBelongingJob.get(0);
        					if(taskType.getCode() == ut.getTaskType()) {
        						TaskOrder taskOrder = new TaskOrder(queue, ut);
        						taskOrder.setTaskCount(taskCount);
        						updateQueuedJobAfterTaskScheduled(queue, ut, job);
        						got = Optional.of(taskOrder);
        					}
        				}
        			}
        		}
        	} catch (Exception e) {
        		LOG.warn("Fail to select an task to schedule: ", e);
        	}
        	
        	// allocate resource
        	if(got.isPresent()) {
        		resourceManager.allocateResource(queue, taskType);
        		resourceManager.currentResourceStatuses();
        		break;
        	}
        }
        return got;
	}
	
    private void updateQueuedJobAfterTaskScheduled(String queue, Task ut, JSONObject job) throws Exception {
    	// (job, task) = (SCHEDULED, SCHEDULED) 
    	job.put(ScheduledConstants.JOB_STATUS, JobStatus.SCHEDULED.toString());
    	job.put(ScheduledConstants.TASK_ID, ut.getId());
    	job.put(ScheduledConstants.SEQ_NO, ut.getSeqNo());
    	job.put(ScheduledConstants.TASK_STATUS, TaskStatus.SCHEDULED.toString());
    	job.put(ScheduledConstants.LAST_UPDATE_TS, Time.now());
    	stateManager.updateQueuedJob(ut.getJobId(), queue, job);
    	LOG.info("Scheduling task selected: " + job);
	}

}
