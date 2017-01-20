package cn.shiyanjun.platform.scheduled.dao.mappers;

import java.util.List;

import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import cn.shiyanjun.platform.scheduled.dao.entities.Task;

public interface TaskMapper {

    int insertTasks(
    		@Param("list") List<Task> tasks);

    int updateTaskById(Task task);


    public List<Task> retrieveTasks(
            @Param("jobIds")List<Integer> jobIds,
            @Param("taskType")int taskType,
            @Param("taskStatus")int  taskStatus);
    
    @Select("SELECT id,job_id AS jobId,status,seq_no AS seqNo,task_type AS taskType,params," +
            "result_count AS resultCount,start_time AS startTime,done_time AS doneTime FROM task WHERE job_id=#{jobId} ORDER BY seqNo ")
    public List<Task> getTasksFor(
            @Param("jobId") int jobId);

}
