package cn.shiyanjun.platform.scheduled.component;

import java.util.List;

import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;

import com.google.common.collect.Lists;

import cn.shiyanjun.platform.scheduled.common.TaskPersistenceService;
import cn.shiyanjun.platform.scheduled.dao.entities.Task;
import cn.shiyanjun.platform.scheduled.dao.mappers.TaskMapper;

public class TaskPersistenceServiceImpl implements TaskPersistenceService {

	private final SqlSessionFactory sqlSessionFactory;
	
	public TaskPersistenceServiceImpl(SqlSessionFactory sqlSessionFactory) {
		super();
		this.sqlSessionFactory = sqlSessionFactory;
	}

	@Override
	public void insertTasks(List<Task> tasks) {
		SqlSession sqlSession = null;
		try {
			sqlSession = sqlSessionFactory.openSession(true);
			TaskMapper taskMapper = sqlSession.getMapper(TaskMapper.class);
			taskMapper.insertTasks(tasks);
		} finally {
			if(sqlSession != null) {
				sqlSession.close();
			}
		}		
	}
	
	@Override
	public void updateTaskByID(Task task) {
		SqlSession sqlSession = null;
		try {
			sqlSession = sqlSessionFactory.openSession(true);
			TaskMapper taskMapper = sqlSession.getMapper(TaskMapper.class);
			taskMapper.updateTaskById(task);
		} finally {
			if(sqlSession != null) {
				sqlSession.close();
			}
		}
	}

	@Override
	public List<Task> getTasksFor(int jobId) {
		List<Task> tasks = Lists.newArrayList();
		SqlSession sqlSession = null;
		try {
			sqlSession = sqlSessionFactory.openSession(true);
			TaskMapper taskMapper = sqlSession.getMapper(TaskMapper.class);
			tasks = taskMapper.getTasksFor(jobId);
		} finally {
			if(sqlSession != null) {
				sqlSession.close();
			}
		}
		return tasks;
	}

}
