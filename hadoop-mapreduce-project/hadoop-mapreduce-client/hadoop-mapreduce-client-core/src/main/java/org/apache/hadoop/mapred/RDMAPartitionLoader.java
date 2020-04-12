package org.apache.hadoop.mapred;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;

public class RDMAPartitionLoader extends Thread {

    private static final Log LOG = LogFactory.getLog(RDMAPartitionLoader.class);

    private final IndexRecord ir;
    private final Path outputFile;
    private final int reducer;

    public RDMAPartitionLoader(IndexRecord ir, Path outputFile, int reducer){
        this.ir = ir;
        this.reducer = reducer;
        this.outputFile = outputFile;

    }

    @Override
    public void run() {
        LOG.info("MapOutputFileName: " + outputFile.toString());
        LOG.info("For reducer: " + reducer);
        LOG.info("startOffset: " + ir.startOffset + " partLength: " + ir.partLength + " rawLength: " + ir.rawLength);
    }
}
