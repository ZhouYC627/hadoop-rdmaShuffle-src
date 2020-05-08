package org.apache.hadoop.mapreduce.rdma;

import java.util.concurrent.atomic.AtomicInteger;

public class RdmaConfigs {
    public static final int LOAD_SIZE = 64 * 1024; // 64KB
    public static final int TOTAL_MEMORY_BLOCK = 10;

    public static final int SEND_RECV_SIZE = 1024; // 1KB
    public static final int TIMEOUT = 1000;
    private static AtomicInteger wrID = new AtomicInteger(1000);

    public static int getNextWrID(){
        return wrID.incrementAndGet();
    }

}
