package cn.shiyanjun.platform.scheduled.dao.entities;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.List;

@SuppressWarnings("serial")
public class Job implements Serializable {
	
    private Integer id;
    private String name;
    private Integer jobType;
    private String params;
    private Integer status;
    private Timestamp createTime;
    private Timestamp doneTime;
    private List<Task> tasks;

    public Job() {
    	super();
    }

    public Job(Integer id, Integer status) {
        this.id = id;
        this.status = status;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

	public Integer getJobType() {
		return jobType;
	}
	
	public void setJobType(Integer jobType) {
		this.jobType = jobType;
	}

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getParams() {
        return params;
    }

    public void setParams(String params) {
        this.params = params;
    }

    public Timestamp getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Timestamp createTime) {
        this.createTime = createTime;
    }

    public Timestamp getDoneTime() {
        return doneTime;
    }

    public void setDoneTime(Timestamp doneTime) {
        this.doneTime = doneTime;
    }

    public List<Task> getTasks() {
        return tasks;
    }

    public void setTasks(List<Task> tasks) {
        this.tasks = tasks;
    }

    @Override
    public String toString() {
        return "Job{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", params='" + params + '\'' +
                ", status=" + status +
                ", createTime=" + createTime +
                ", doneTime=" + doneTime +
                ", tasks=" + tasks +
                '}';
    }

}
