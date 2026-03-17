package io.openlineage.datastage.dsodb.repository;

import java.sql.Timestamp;

public record JobExecRow(
        String jobName,
        String projectName,
        int waveNo,
        int jobStatus,
        Timestamp runStartTime,
        Timestamp runEndTime) {

    // DSODB status codes: 1=Running, 2=Finished, 3=Finished+warnings, 4=Aborted, 5=Stopped
    public boolean isRunning()  { return jobStatus == 1; }
    public boolean isFinished() { return jobStatus == 2 || jobStatus == 3; }
    public boolean isAborted()  { return jobStatus == 4; }
    public boolean isStopped()  { return jobStatus == 5; }
    public boolean isTerminal() { return isFinished() || isAborted() || isStopped(); }
    public boolean isSuccess()  { return jobStatus == 2 || jobStatus == 3; }
}
