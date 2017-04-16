package cn.shiyanjun.platform.scheduled.dao.entities;

import java.io.Serializable;
import java.sql.Timestamp;

@SuppressWarnings("serial")
public class Task implements Serializable {
	
    private Integer id;
    private Integer jobId;
    private Integer status;
    private Integer seqNo;
    private Integer taskType;
    private String params;
    private Integer resultCount;
    private Timestamp startTime;
    private Timestamp doneTime;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getJobId() {
        return jobId;
    }

    public void setJobId(Integer jobId) {
        this.jobId = jobId;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public Integer getSeqNo() {
        return seqNo;
    }

    public void setSeqNo(Integer seqNo) {
        this.seqNo = seqNo;
    }

    public Integer getTaskType() {
        return taskType;
    }

    public void setTaskType(Integer taskType) {
        this.taskType = taskType;
    }

    public Integer getResultCount() {
        return resultCount;
    }

    public void setResultCount(Integer resultCount) {
        this.resultCount = resultCount;
    }

    public String getParams() {
        return params;
    }

    public void setParams(String params) {
        this.params = params;
    }


    public Timestamp getStartTime() {
        return startTime;
    }

    public void setStartTime(Timestamp startTime) {
        this.startTime = startTime;
    }

    public Timestamp getDoneTime() {
        return doneTime;
    }

    public void setDoneTime(Timestamp doneTime) {
        this.doneTime = doneTime;
    }


    @Override
    public String toString() {
        return "Task{" +
                "id=" + id +
                ", jobId=" + jobId +
                ", status=" + status +
                ", seqNo=" + seqNo +
                ", taskType=" + taskType +
                ", params='" + params + '\'' +
                ", resultCount=" + resultCount +
                ", startTime=" + startTime +
                ", doneTime=" + doneTime +
                '}';
    }

}
