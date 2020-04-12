package org.apache.hadoop.mapreduce.task.reduce;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import javax.crypto.SecretKey;
import java.util.Map;

public class RDMAFetcher extends LocalFetcher {
    public RDMAFetcher(JobConf job, TaskAttemptID reduceId, ShuffleSchedulerImpl scheduler, MergeManager merger, Reporter reporter, ShuffleClientMetrics metrics, ExceptionReporter exceptionReporter, SecretKey shuffleKey, Map localMapFiles) {
        super(job, reduceId, scheduler, merger, reporter, metrics, exceptionReporter, shuffleKey, localMapFiles);
    }
}
