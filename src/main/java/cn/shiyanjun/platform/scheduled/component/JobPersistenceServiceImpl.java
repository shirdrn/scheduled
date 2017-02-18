package cn.shiyanjun.platform.scheduled.component;

import java.util.List;

import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;

import com.google.common.collect.Lists;

import cn.shiyanjun.platform.api.constants.JobStatus;
import cn.shiyanjun.platform.scheduled.api.JobPersistenceService;
import cn.shiyanjun.platform.scheduled.dao.entities.Job;
import cn.shiyanjun.platform.scheduled.dao.mappers.JobMapper;

public class JobPersistenceServiceImpl implements JobPersistenceService {

	private final SqlSessionFactory sqlSessionFactory;
	
	public JobPersistenceServiceImpl(SqlSessionFactory sqlSessionFactory) {
		super();
		this.sqlSessionFactory = sqlSessionFactory;
	}

	@Override
	public List<Job> getJobByState(JobStatus jobStatus) {
		List<Job> jobs = Lists.newArrayList();
		SqlSession sqlSession = null;
		try {
			sqlSession = sqlSessionFactory.openSession(true);
			JobMapper jobMapper = sqlSession.getMapper(JobMapper.class);
			jobs = jobMapper.getJobByState(jobStatus.getCode());
		} finally {
			if(sqlSession != null) {
				sqlSession.close();
			}
		}
		return jobs;
	}

	@Override
	public void updateJobByID(Job job) {
		SqlSession sqlSession = null;
		try {
			sqlSession = sqlSessionFactory.openSession(true);
			JobMapper jobMapper = sqlSession.getMapper(JobMapper.class);
			jobMapper.updateJobById(job);
		} finally {
			if(sqlSession != null) {
				sqlSession.close();
			}
		}
	}
	
	@Override
	public Job retrieveJob(int jobId) {
		Job job = new Job();
		SqlSession sqlSession = null;
		try {
			sqlSession = sqlSessionFactory.openSession(true);
			JobMapper jobMapper = sqlSession.getMapper(JobMapper.class);
			job = jobMapper.getJobById(jobId);
		} finally {
			if(sqlSession != null) {
				sqlSession.close();
			}
		}
		return job;
	}

}
