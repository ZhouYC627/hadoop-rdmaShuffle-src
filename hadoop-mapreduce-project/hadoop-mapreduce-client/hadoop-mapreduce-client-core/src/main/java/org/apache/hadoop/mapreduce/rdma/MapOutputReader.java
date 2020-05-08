package org.apache.hadoop.mapreduce.rdma;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.concurrent.*;

public class MapOutputReader {

    final int BLOCK_SIZE = 64*1024;
    final int NUMBER_OF_THREAD = 3;

    private String outputFileName;
    private ArrayList<IndexRecord> indexRecords;
    private long[] currentPos;
    private FileChannel[] channels;

    private ExecutorService executor;

    public MapOutputReader(String outputFileName, ArrayList<IndexRecord> indexList) throws IOException {
        this.outputFileName = outputFileName;
        this.indexRecords = indexList;
        currentPos = new long[indexList.size()];
        channels = new FileChannel[indexList.size()];

        for (int i = 0; i < indexList.size(); i++){
            currentPos[i] = (indexList.get(i).startOffset);
            channels[i] = FileChannel.open(Paths.get(outputFileName));
            channels[i].position(currentPos[i]);
        }
        executor = Executors.newFixedThreadPool(NUMBER_OF_THREAD);

    }

    public Future<Integer> getBlockFuture(int reduce, ByteBuffer buf) throws IOException {

        //ByteBuffer buf = ByteBuffer.allocate(BLOCK_SIZE);
        if (reduce >= indexRecords.size()){
            throw new IOException("No such partition for reducer " + reduce);
        }
        System.out.println("Reading " + BLOCK_SIZE + " from position " + currentPos[reduce] + " for reduce " + reduce);
        Callable<Integer> readTask = new ReadBlock(channels[reduce], buf);
        Future<Integer> len = executor.submit(readTask);
        currentPos[reduce] += BLOCK_SIZE;
        return len;

    }

    public String getOutputFileName(){
        return outputFileName;
    }

    class ReadBlock implements Callable<Integer> {
        FileChannel channel;
        ByteBuffer buffer;

        public ReadBlock(FileChannel channel, ByteBuffer buf) {
            this.channel = channel;
            this.buffer = buf;
        }

        @Override
        public Integer call() throws Exception {

            return channel.read(buffer);
        }
    }

}
