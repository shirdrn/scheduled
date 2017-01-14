package cn.shiyanjun.platform.scheduled.protocols;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.collect.Lists;

import cn.shiyanjun.platform.api.Context;
import cn.shiyanjun.platform.api.constants.JobStatus;
import cn.shiyanjun.platform.scheduled.api.ComponentManager;
import cn.shiyanjun.platform.scheduled.api.Protocol;
import cn.shiyanjun.platform.scheduled.common.AbstractProtocolManager;
import cn.shiyanjun.platform.scheduled.constants.ProtocolType;
import cn.shiyanjun.platform.scheduled.dao.entities.Job;

public final class JobFetchProtocolManager extends AbstractProtocolManager<Protocol<JobStatus, List<Job>>> {

	private static final Log LOG = LogFactory.getLog(JobFetchProtocolManager.class);
	private final ComponentManager manager;
	public JobFetchProtocolManager(ComponentManager grm, Context context) {
		super(context);
		manager = grm;
	}

	@Override
	public void initialize() {
		super.register(ProtocolType.MYSQL, new FetchSubmittedJob());
	}
	
	private class FetchSubmittedJob implements Protocol<JobStatus, List<Job>> {

		@Override
		public List<Job> request(JobStatus in) {
			List<Job> submittedJobs = Lists.newArrayList();
			try {
				submittedJobs = manager.getJobPersistenceService().getJobByState(in);
			} catch (Exception e) {
				LOG.warn("Failed to fetch job: ", e);
			}
			return submittedJobs;
		}
		
	}
	
	@Override
	public Enum<?> ofType(String protocolType) {
		return ProtocolType.valueOf(protocolType);
	}

}
