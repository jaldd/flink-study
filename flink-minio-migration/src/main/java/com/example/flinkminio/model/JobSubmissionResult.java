package com.example.flinkminio.model;

/**
 * 作业提交结果
 *
 * 用于 REST API 返回作业提交的状态信息
 */
public class JobSubmissionResult {

    /**
     * 作业是否提交成功
     */
    private boolean success;

    /**
     * 结果消息
     */
    private String message;

    /**
     * 作业名称
     */
    private String jobName;

    /**
     * Checkpoint 存储路径
     */
    private String checkpointPath;

    public JobSubmissionResult() {
    }

    public JobSubmissionResult(boolean success, String message, String jobName, String checkpointPath) {
        this.success = success;
        this.message = message;
        this.jobName = jobName;
        this.checkpointPath = checkpointPath;
    }

    public static JobSubmissionResult success(String jobName, String checkpointPath) {
        return new JobSubmissionResult(true, "作业提交成功", jobName, checkpointPath);
    }

    public static JobSubmissionResult failure(String message) {
        return new JobSubmissionResult(false, message, null, null);
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
