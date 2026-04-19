package com.example.flinkminio.model;

public class JobSubmissionResult {

    private boolean success;
    private String message;
    private String jobId;
    private String jobName;
    private String checkpointPath;

    public JobSubmissionResult() {
    }

    public JobSubmissionResult(boolean success, String message, String jobId, String jobName, String checkpointPath) {
        this.success = success;
        this.message = message;
        this.jobId = jobId;
        this.jobName = jobName;
        this.checkpointPath = checkpointPath;
    }

    public static JobSubmissionResult success(String jobId, String jobName, String checkpointPath) {
        return new JobSubmissionResult(true, "作业提交成功", jobId, jobName, checkpointPath);
    }

    public static JobSubmissionResult failure(String message) {
        return new JobSubmissionResult(false, message, null, null, null);
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getCheckpointPath() {
        return checkpointPath;
    }

    public void setCheckpointPath(String checkpointPath) {
        this.checkpointPath = checkpointPath;
    }
}
