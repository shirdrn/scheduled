package cn.shiyanjun.platform.scheduled.common;

import java.util.List;

import cn.shiyanjun.platform.api.constants.JobStatus;
import cn.shiyanjun.platform.scheduled.dao.entities.Job;

public interface JobPersistenceService {

	List<Job> getJobByState(JobStatus jobStatus);
	
	void updateJobByID(Job job);
	
	Job retrieveJob(int jobId);
}
