package org.apache.hadoop.mapreduce.rdma;

import com.ibm.disni.RdmaActiveEndpoint;
import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.util.DiSNILogger;
import com.ibm.disni.verbs.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.concurrent.ArrayBlockingQueue;

public class MapperEndpoint extends RdmaActiveEndpoint {
    private ByteBuffer dataBuffer; // used receive the data that read from Mapper
    private ByteBuffer recvBuffer; // used to receive the memory information from Mapper

    private IbvMr dataMr;
    private IbvMr recvMr;

    private IbvSendWR writeWR;
    private IbvRecvWR recvWR;

    private LinkedList<IbvRecvWR> recvWR_list;
    private LinkedList<IbvSendWR> writeWR_list;

    // scattered/gathered element
    private IbvSge sgeWrite;
    private LinkedList<IbvSge> sgeList_write;

    private IbvSge sgeRecv;
    private LinkedList<IbvSge> sgeList_recv;

    private ArrayBlockingQueue<IbvWC> receiveCompletionEvents;
    private ArrayBlockingQueue<IbvWC> writingCompletionEvents;
    private ArrayBlockingQueue<MapperEndpoint> pendingRequestsFromReducer;

    //TODO only for testing
    private int recvId = 0;
    private int sendId = 10;

    public MapperEndpoint(RdmaActiveEndpointGroup<? extends MapperEndpoint> endpointGroup, RdmaCmId idPriv, boolean serverSide) throws IOException {
        super(endpointGroup, idPriv, serverSide);

        this.dataBuffer = ByteBuffer.allocateDirect(64 * 1024);
        this.recvBuffer = ByteBuffer.allocateDirect(RdmaConfigs.SEND_RECV_SIZE);

        this.recvWR = new IbvRecvWR();
        this.writeWR =  new IbvSendWR();

        this.recvWR_list = new LinkedList<>();
        this.writeWR_list = new LinkedList<>();

        this.sgeRecv = new IbvSge();
        this.sgeWrite = new IbvSge();

        this.sgeList_recv = new LinkedList<>();
        this.sgeList_write = new LinkedList<>();

        receiveCompletionEvents = new ArrayBlockingQueue<>(100);
        writingCompletionEvents = new ArrayBlockingQueue<>(100);
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

        postRecv(recvWR_list).execute().free();
        DiSNILogger.getLogger().info("PostRecv!");

    }

    public void executeRDMAWrite(long addr1, int lkey1) throws IOException {
        this.writeWR_list.clear();
        this.sgeList_write.clear();

        sgeWrite.setAddr(dataMr.getAddr()); // address of the local data buffer that the data will be gathered from or scattered to.
        sgeWrite.setLength(dataMr.getLength()); // the size of the data that will be read from / written to this address.
        sgeWrite.setLkey(dataMr.getLkey()); //  the local key of the MR that was registered to this buffer.
        sgeList_write.add(sgeWrite);

        writeWR.setWr_id(sendId++);  // user-defined identifier that will be returned in the completion element for this work request
        writeWR.setSg_list(sgeList_write); //sgeList: a pointer to a list of buffers and their sizes where the data to be transmitted is located
        writeWR.setOpcode(IbvSendWR.IBV_WR_RDMA_WRITE_WITH_IMM); // describe type of the operation: here is SEND operation
        writeWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
        writeWR.getRdma().setRemote_addr(addr1); // set remote address
        writeWR.getRdma().setRkey(lkey1); // set remote key
        writeWR_list.add(writeWR);


        postSend(writeWR_list).execute().free();
        DiSNILogger.getLogger().info("RDMA Write!");
    }


    @Override
    public void init() throws IOException {
        super.init();

        // register the memory regions of the data buffer
        dataMr = registerMemory(dataBuffer).execute().free().getMr();
        recvMr = registerMemory(recvBuffer).execute().free().getMr();

        // init a RECEIVE request
        this.executePostRecv();
        DiSNILogger.getLogger().info("Init postRecv");

    }

    public void initReceiving(ArrayBlockingQueue<MapperEndpoint> pendingRequestsFromReducer) throws IOException {
        this.pendingRequestsFromReducer = pendingRequestsFromReducer;
        DiSNILogger.getLogger().info("Init RequestQueue");
    }

    @Override
    public void dispatchCqEvent(IbvWC wc) throws IOException {
        if (IbvWC.IbvWcOpcode.valueOf(wc.getOpcode()).equals(IbvWC.IbvWcOpcode.IBV_WC_RDMA_WRITE)) {
            DiSNILogger.getLogger().info("RMDA Write with IMM Completed!");
            writingCompletionEvents.add(wc); // this wc events for send events

        }
        else if (IbvWC.IbvWcOpcode.valueOf(wc.getOpcode()).equals(IbvWC.IbvWcOpcode.IBV_WC_RECV)){
            DiSNILogger.getLogger().info("Recv Completion WR in RequestQueue!");
            pendingRequestsFromReducer.add(this);
            //TODO testing
            receiveCompletionEvents.add(wc);

        }
    }

    public ArrayBlockingQueue<IbvWC> getReceiveCompletionEvents() {
        return receiveCompletionEvents;
    }

    public ArrayBlockingQueue<IbvWC> getWritingCompletionEvents() { return writingCompletionEvents;}

    public ByteBuffer getDataBuf() {
        return dataBuffer;
    }

    public ByteBuffer getRecvBuf() {
        return recvBuffer;
    }

}

















