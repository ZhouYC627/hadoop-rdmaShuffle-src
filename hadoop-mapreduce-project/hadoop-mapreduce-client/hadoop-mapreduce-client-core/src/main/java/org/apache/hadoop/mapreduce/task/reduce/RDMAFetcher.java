package org.apache.hadoop.mapreduce.task.reduce;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.CryptoUtils;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.rdma.RdmaDataInputStream;
import org.apache.hadoop.mapreduce.rdma.RDMAClient;
import org.apache.hadoop.util.Time;
import org.apache.log4j.BasicConfigurator;

import javax.crypto.SecretKey;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.*;

public class RDMAFetcher<K, V> extends Fetcher<K, V> {

    private static final Log LOG = LogFactory.getLog(RDMAFetcher.class);
    private static final int DEFAULT_PORT = 1919;

    private JobConf job;

    public RDMAFetcher(JobConf job, TaskAttemptID reduceId, ShuffleSchedulerImpl scheduler, MergeManager merger, Reporter reporter, ShuffleClientMetrics metrics, ExceptionReporter exceptionReporter, SecretKey shuffleKey) {
        super(job, reduceId, scheduler, merger, reporter, metrics,
                exceptionReporter, shuffleKey);
        this.job = job;
        setName("rdmaFetcher#" + id);
        setDaemon(true);
    }

    @Override
    public void run() {
        try {
            while (!isStopped() && !Thread.currentThread().isInterrupted()) {
                MapHost host = null;
                try {
                    // If merge is on, block
                    merger.waitForResource();

                    // Get a host to shuffle from
                    host = scheduler.getHost();
                    metrics.threadBusy();

                    // Shuffle
                    rdmaFromHost(host, reduce);
                } finally {
                    if (host != null) {
                        scheduler.freeHost(host);
                        metrics.threadFree();
                    }
                }
            }
        } catch (InterruptedException ie) {
            LOG.error(ie);
        } catch (Throwable t) {
            exceptionReporter.reportException(t);
        }
    }

    private void rdmaFromHost(MapHost host, int reduceId) throws Exception {
        String hostIP = host.getHostName();
        hostIP = hostIP.substring(0, hostIP.indexOf(':'));
        LOG.info("HostIP: " + hostIP);

        // Get completed maps on 'host'
        List<TaskAttemptID> maps = scheduler.getMapsForHost(host);

        // Sanity check to catch hosts with only 'OBSOLETE' maps,
        // especially at the tail of large jobs
        if (maps.size() == 0) {
            LOG.info("No mapOutput to fetch");
            return;
        }

        BasicConfigurator.configure();
        RDMAClient fetcherClient = new RDMAClient();
        RdmaDataInputStream rdmaInput = fetcherClient.createRdmaStream(hostIP, DEFAULT_PORT);

        if (rdmaInput == null){
            LOG.error("RDMA InputStream is NULL!");
            return;
        }
        LOG.info("RDMA Connected to server: " + hostIP);

        Iterator<TaskAttemptID> iter = maps.iterator();
        while (iter.hasNext()){
            TaskAttemptID map = iter.next();
            LOG.info("RDMAFetcher " + id + " going to fetch from " + hostIP + " for: "
                    + map);

            ShuffleHeader shuffleHeader = getShuffleHeader(host, maps);
            if (copyMapOutput(host, map, shuffleHeader, rdmaInput)) {
                // Successful copy. Remove this from our worklist.
                iter.remove();
            }else {
                // We got back a WAIT command; go back to the outer loop
                // and block for InMemoryMerge.
                LOG.warn("copyMapOutput failed for tasks " + maps);
                scheduler.hostFailed(host.getHostName());
                scheduler.copyFailed(map, host, true, false);
                break;
            }
        }
        rdmaInput.closeEndpoint();
        fetcherClient.closeEndpointGroup();
    }

    private ShuffleHeader getShuffleHeader(MapHost host, List<TaskAttemptID> maps) throws IOException {
        ShuffleHeader header = new ShuffleHeader();
        URL url = getMapOutputURL(host, maps);
        DataInputStream input = openShuffleUrl(host, new HashSet<TaskAttemptID>(maps), url);

        header.readFields(input);
        return header;
    }

    private boolean copyMapOutput(MapHost host, TaskAttemptID map, ShuffleHeader header, RdmaDataInputStream input) {
        long decompressedLength = header.uncompressedLength;
        long compressedLength = header.compressedLength;
        int forReduce = header.forReduce;
        TaskAttemptID mapId = TaskAttemptID.forName(header.mapId);
        MapOutput<K,V> mapOutput = null;

        try {
            long startTime = Time.monotonicNow();

            input.prepareInfo(mapId.getTaskID().getId(), forReduce);

            InputStream is = CryptoUtils.wrapIfNecessary(job, input, compressedLength);
            compressedLength -= CryptoUtils.cryptoPadding(job);
            decompressedLength -= CryptoUtils.cryptoPadding(job);

            // Do some basic sanity verification
            if (!verifySanity(compressedLength, decompressedLength, forReduce, map, mapId)){
                return false;
            }


            LOG.info("header: " + mapId + ", len: " + compressedLength +
                    ", decomp len: " + decompressedLength);

            // Get the location for the map output - either in-memory or on-disk
            mapOutput = merger.reserve(mapId, decompressedLength, id);

            if (mapOutput == null) {
                LOG.info("RDMAfetcher#" + id + " - MergeManager returned status WAIT ...");
                //Not an error but wait to process data.
                return false;
            }

            LOG.info("fetcher#" + id + " about to shuffle output of map "
                    + mapOutput.getMapId() + " decomp: " + decompressedLength
                    + " len: " + compressedLength + " to " + mapOutput.getDescription());
            mapOutput.shuffle(host, is, compressedLength, decompressedLength,
                    metrics, reporter);

            // Inform the shuffle scheduler
            long endTime = Time.monotonicNow();
            // Reset retryStartTime as map task make progress if retried before.

            scheduler.copySucceeded(mapId, host, compressedLength,
                    startTime, endTime, mapOutput);
            metrics.successFetch();

        } catch (IOException ioe) {
            scheduler.reportLocalError(ioe);
            return false;
        }

        return true; // successful fetch.

    }

    private boolean verifySanity(long compressedLength, long decompressedLength,
                                 int forReduce, TaskAttemptID map, TaskAttemptID mapId) {
        if (compressedLength < 0 || decompressedLength < 0) {
            LOG.warn(getName() + " invalid lengths in map output header: id: " +
                    mapId + " len: " + compressedLength + ", decomp len: " +
                    decompressedLength);
            return false;
        }

        if (forReduce != reduce) {
            LOG.warn(getName() + " data for the wrong reduce map: " +
                    mapId + " len: " + compressedLength + " decomp len: " +
                    decompressedLength + " for reduce " + forReduce);
            return false;
        }

        // Sanity check
        if (!map.equals(mapId)) {
            LOG.warn("Invalid map-output! Received output for " + mapId);
            return false;
        }

        return true;
    }
}
