package cn.shiyanjun.platform.scheduled.dao.mappers;

import java.util.List;

import cn.shiyanjun.platform.scheduled.dao.entities.Job;

public interface JobMapper {

    int updateJobById(Job job);

    List<Job> getJobByState(Integer state);

    Job getJobById(Integer jobId);
}
