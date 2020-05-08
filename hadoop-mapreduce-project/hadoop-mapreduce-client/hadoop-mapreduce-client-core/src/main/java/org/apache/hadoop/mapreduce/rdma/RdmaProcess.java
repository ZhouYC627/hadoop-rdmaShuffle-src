package org.apache.hadoop.mapreduce.rdma;

import com.ibm.disni.util.DiSNILogger;
import com.ibm.disni.verbs.IbvWC;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class RdmaProcess implements Runnable{
    private ArrayBlockingQueue<MapperEndpoint> pendingRequestsFromReducer;
    private Hashtable<Integer, MapOutputReader> readers;
    private List<Future<Integer>> bufferFutures;
    private final Logger LOGGER;

    RdmaProcess(ArrayBlockingQueue<MapperEndpoint> pendingRequestsFromReducer, Hashtable<Integer, MapOutputReader> readers) {
        this.LOGGER =  DiSNILogger.getLogger();

        this.pendingRequestsFromReducer = pendingRequestsFromReducer;
        this.readers = readers;
        this.bufferFutures = new ArrayList<>();
    }

    @Override
    public void run() {
        while (true) {
            try {
                DiSNILogger.getLogger().info("Rdma Process running.... waiting for Requests");
                MapperEndpoint endpoint = pendingRequestsFromReducer.take();

                // Wait until the outputfile is ready
                if (readers.isEmpty()){
                    Thread.sleep(100);
                    continue;
                }

                //TODO remove After testing
                IbvWC recWc = endpoint.getReceiveCompletionEvents().take();
                DiSNILogger.getLogger().info("Recv wr_id: " + recWc.getWr_id() + " op: " + recWc.getOpcode());
                // Post another recv on this endpoint for further messages
                endpoint.executePostRecv();

                ByteBuffer recvBuffer = endpoint.getRecvBuf();
                recvBuffer.clear();
                long addr = recvBuffer.getLong();
                int lkey = recvBuffer.getInt();
                int mapperId = recvBuffer.getInt();
                int reducerId = recvBuffer.getInt();
                recvBuffer.clear();
                DiSNILogger.getLogger().info("information received, mapperId " + mapperId + " for reducerId " + reducerId);

                // check if the reader has been processed for this mapperId
                if (!readers.containsKey(mapperId)){
                    LOGGER.info("Cannot find MapOutputFile: " + mapperId);
                    continue;
                }
                MapOutputReader reader = readers.get(mapperId);

                ByteBuffer dataBuf = endpoint.getDataBuf();
                dataBuf.clear();
                //dataBuf.asCharBuffer().put("This is Required data of mapperId " + mapperId + " for reducerId " + reducerId);
                //String msg = "This is Required data of mapperId " + mapperId + " for reducerId " + reducerId;
                //byte[] msgArr = msg.getBytes();
                //DiSNILogger.getLogger().info("msg len: " + msgArr.length);
                //dataBuf.put(msgArr);

                // get(reducerId, sendBuf) return Future
                //bufferFutures.add(reader.getBlockFuture(reducerId, dataBuf));
                Future<Integer> res = reader.getBlockFuture(reducerId, dataBuf);
                bufferFutures.add(res);
                LOGGER.info(String.valueOf(res.isDone()));
                LOGGER.info("Read from MapOutputFile. length: "+ bufferFutures.get(bufferFutures.size()-1).get());

                //TODO need to know the size of the data
                endpoint.executeRDMAWrite(addr, lkey);

                DiSNILogger.getLogger().info("MapperClient::write memory to server " + endpoint.getDstAddr());
                IbvWC writeWC = endpoint.getWritingCompletionEvents().take();
                //TODO
                DiSNILogger.getLogger().info("Send wr_id: " + writeWC.getWr_id() + " op: " + writeWC.getOpcode());


//                endpoint.close();
//                DiSNILogger.getLogger().info("closing endpoint, done");

            } catch (InterruptedException | IOException | ExecutionException e) {
                e.printStackTrace();
            }
        }

    }
}
