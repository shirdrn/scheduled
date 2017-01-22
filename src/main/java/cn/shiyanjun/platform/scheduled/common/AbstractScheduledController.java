package cn.shiyanjun.platform.scheduled.common;

import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.collect.Sets;

import cn.shiyanjun.platform.api.Context;
import cn.shiyanjun.platform.api.common.AbstractComponent;
import cn.shiyanjun.platform.scheduled.api.ScheduledController;

public abstract class AbstractScheduledController extends AbstractComponent implements ScheduledController {

	private static final Log LOG = LogFactory.getLog(AbstractScheduledController.class);
	private static final Set<Integer> cancellingJobs = Sets.newConcurrentHashSet();
	protected volatile boolean isSchedulingOpened;
	
	public AbstractScheduledController(Context context) {
		super(context);
	}
	
	@Override
	public boolean cancelJob(int jobId) {
		LOG.info("Prepare to cancel job: jobId=" + jobId);
		return cancellingJobs.add(jobId);
	}

	@Override
	public boolean shouldCancelJob(int jobId) {
		return cancellingJobs.contains(jobId);
	}

	@Override
	public void jobCancelled(int jobId, Runnable action) {
		action.run();
		cancellingJobs.remove(jobId);
		LOG.info("Job cancelled: jobId=" + jobId);
	}
	
	
	@Override
	public boolean isSchedulingOpened() {
		return isSchedulingOpened;
	}

	@Override
	public void setSchedulingOpened(boolean isSchedulingOpened) {
		this.isSchedulingOpened = isSchedulingOpened;
	}
}
