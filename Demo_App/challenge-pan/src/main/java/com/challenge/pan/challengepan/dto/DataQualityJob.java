package com.challenge.pan.challengepan.dto;

import com.amazonaws.services.glue.model.JobRun;

import javax.xml.crypto.Data;
import java.util.Date;

public class DataQualityJob {

    private String jobId;
    private String name;
    private String status;
    private Date startTime;
    private Date endTime;
    private String errorMsg;

    public DataQualityJob(JobRun job) {
        setJobId(job.getId());
        this.name = job.getJobName();
        this.startTime = job.getStartedOn();
        this.endTime = job.getCompletedOn();
        this.errorMsg = job.getErrorMessage();
        this.setStatus(job.getJobRunState());
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {

        this.jobId = jobId.substring(0,10) + "...";
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        if(status.equalsIgnoreCase("TIMEOUT")){
            this.status = "COMPLETED";
        }else{
            this.status = status;
        }

    }

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }
}
