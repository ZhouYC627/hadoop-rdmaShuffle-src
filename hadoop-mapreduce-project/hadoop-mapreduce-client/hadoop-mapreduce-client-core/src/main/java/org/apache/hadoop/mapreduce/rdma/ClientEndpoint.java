package org.apache.hadoop.mapreduce.rdma;

import com.ibm.disni.RdmaActiveEndpoint;
import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.util.DiSNILogger;
import com.ibm.disni.verbs.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.concurrent.ArrayBlockingQueue;

public class ClientEndpoint extends RdmaActiveEndpoint {
    private ByteBuffer dataBuffer; // used receive the data that read from Mapper
    private ByteBuffer sendBuffer; // used to contain file information that send to Mapper
    private ByteBuffer recvBuffer; // used to receive the memory information from Mapper

    private IbvMr dataMr;
    private IbvMr sendMr;
    private IbvMr recvMr;

    private IbvSendWR sendWR;
    private IbvRecvWR recvWR;

    private LinkedList<IbvSendWR> sendWR_list;
    private LinkedList<IbvRecvWR> recvWR_list;

    private IbvSge sgeSend;
    private LinkedList<IbvSge> sgeList_send;

    private IbvSge sgeRecv;
    private LinkedList<IbvSge> sgeList_recv;

    private ArrayBlockingQueue<IbvWC> sendCompletionEvents;
    private ArrayBlockingQueue<IbvWC> writeCompletionEvents;

    //TODO only for testing
    private int recvId = 0;
    private int sendId = 10;

    public ClientEndpoint(RdmaActiveEndpointGroup<? extends RdmaActiveEndpoint> endpointGroup, RdmaCmId idPriv, boolean serverSide) throws IOException {
        super(endpointGroup, idPriv, serverSide);

        this.dataBuffer = ByteBuffer.allocateDirect(RdmaConfigs.LOAD_SIZE);
        this.sendBuffer = ByteBuffer.allocateDirect(RdmaConfigs.SEND_RECV_SIZE);
        this.recvBuffer = ByteBuffer.allocateDirect(RdmaConfigs.SEND_RECV_SIZE);

        this.sendWR = new IbvSendWR();
        this.recvWR = new IbvRecvWR();

        this.sendWR_list = new LinkedList<>();
        this.recvWR_list = new LinkedList<>();

        this.sgeSend = new IbvSge();
        this.sgeRecv = new IbvSge();

        this.sgeList_send = new LinkedList<>();
        this.sgeList_recv = new LinkedList<>();

        sendCompletionEvents = new ArrayBlockingQueue<>(100);
        writeCompletionEvents = new ArrayBlockingQueue<>(100);
    }


    public void executePostRecv() throws IOException {
        this.recvWR_list.clear();
        this.sgeList_recv.clear();

        sgeRecv.setAddr(recvMr.getAddr());
        sgeRecv.setLength(recvMr.getLength());
        sgeRecv.setLkey(recvMr.getLkey());
        sgeList_recv.add(sgeRecv);

        recvWR.setWr_id(recvId++);
        recvWR.setSg_list(sgeList_recv);

        recvWR_list.add(recvWR);

        postRecv(recvWR_list).execute();
        DiSNILogger.getLogger().info("PostRecv!");

    }

    public void executePostSend() throws IOException {
        this.sendWR_list.clear();
        this.sgeList_send.clear();

        sgeSend.setAddr(sendMr.getAddr());
        sgeSend.setLength(sendMr.getLength());
        sgeSend.setLkey(sendMr.getLkey());
        sgeList_send.add(sgeSend);

        sendWR.setWr_id(sendId++);
        sendWR.setSg_list(sgeList_send);
        sendWR.setOpcode(IbvSendWR.IBV_WR_SEND);
        sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
        sendWR_list.add(sendWR);

        postSend(sendWR_list).execute();
        DiSNILogger.getLogger().info("PostSend!");
    }

    @Override
    public void init() throws IOException {
        super.init();

        dataMr = registerMemory(dataBuffer).execute().free().getMr();
        sendMr = registerMemory(sendBuffer).execute().free().getMr();
        recvMr = registerMemory(recvBuffer).execute().free().getMr();

        // init a RECEIVE request
        this.executePostRecv();

    }

    @Override
    public void dispatchCqEvent(IbvWC wc) throws IOException {
        if (IbvWC.IbvWcOpcode.valueOf(wc.getOpcode()).equals(IbvWC.IbvWcOpcode.IBV_WC_SEND)) {
            DiSNILogger.getLogger().info("SEND opcode: " + wc.getOpcode());
            sendCompletionEvents.add(wc);
        } else if (IbvWC.IbvWcOpcode.valueOf(wc.getOpcode()).equals(IbvWC.IbvWcOpcode.IBV_WC_RECV_RDMA_WITH_IMM)){
            DiSNILogger.getLogger().info("WRITE_WITH_IMM opcode: " + wc.getOpcode());
            writeCompletionEvents.add(wc);
        }
    }

    public ArrayBlockingQueue<IbvWC> getSendCompletionEvents() {
        return sendCompletionEvents;
    }
    public ArrayBlockingQueue<IbvWC> getWriteCompletionEvents() {
        return writeCompletionEvents;
    }

    public ByteBuffer getDataBuf() {
        return dataBuffer;
    }

    public ByteBuffer getSendBuf() {
        return sendBuffer;
    }

    public IbvMr getDataMr() {
        return dataMr;
    }
}
