package org.apache.hadoop.mapreduce.rdma;

import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.util.DiSNILogger;
import com.ibm.disni.verbs.*;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

public class RdmaDataInputStream extends InputStream {
    private ClientEndpoint endpoint;
    private int mapperId;
    private int reducerId;
    private int length;

    public RdmaDataInputStream(RdmaActiveEndpointGroup<ClientEndpoint> endpointGroup, InetSocketAddress address) throws Exception {
        endpoint = endpointGroup.createEndpoint();
        endpoint.connect(address, RdmaConfigs.TIMEOUT);
        InetSocketAddress _addr = (InetSocketAddress) endpoint.getDstAddr();
        DiSNILogger.getLogger().info("client connected, address " + _addr.toString());
    }

    public void prepareInfo(int mapperId, int reducerId, int length) throws IOException {
        this.mapperId = mapperId;
        this.reducerId = reducerId;
        this.length = length;
        updateInfo();
    }

    public void updateInfo(){
        ByteBuffer sendBuffer = endpoint.getSendBuf();
        IbvMr dataMemoryRegion = endpoint.getDataMr();
        sendBuffer.clear();
        sendBuffer.putLong(dataMemoryRegion.getAddr());
        sendBuffer.putInt(dataMemoryRegion.getLkey());
        sendBuffer.putInt(this.mapperId);
        sendBuffer.putInt(this.reducerId);
        sendBuffer.putInt(this.length);
        sendBuffer.clear();
    }
    @Override
    public int read() throws IOException {
        return 0;
    }

    int i = 0;
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        this.length = len;
        updateInfo();
        int bytesWritten = 0;

        try {
            endpoint.executePostSend();

            // wait until the RDMA SEND message to be sent
            IbvWC sendWc = endpoint.getSendCompletionEvents().take();
            DiSNILogger.getLogger().info("Send wr_id: " + sendWc.getWr_id() + " op: " + sendWc.getOpcode());
            DiSNILogger.getLogger().info("Sending" + i + " completed");

            // wait for the receive buffer received immediate value
            IbvWC recWc = endpoint.getWriteCompletionEvents().take();
            DiSNILogger.getLogger().info("wr_id: " + recWc.getWr_id() + " op: " + recWc.getOpcode());
            endpoint.executePostRecv();
            ByteBuffer dataBuf = endpoint.getDataBuf();

            DiSNILogger.getLogger().info("rdma.Client::Write" + i++ + " Completed notified by the immediate value");
            dataBuf.clear();
            bytesWritten = Math.min(len, dataBuf.limit());
            dataBuf.get(b, 0, bytesWritten);
            DiSNILogger.getLogger().info("rdma.Client::memory is written by server: " + bytesWritten);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return bytesWritten;
    }
    public void close() throws IOException {
        try {
            this.closeEndpoint();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        super.close();
    }

    public void closeEndpoint() throws IOException, InterruptedException {
        endpoint.close();
    }
}
