package org.apache.hadoop.mapreduce.rdma;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class MapOutputReadServer extends Thread {
    private ServerSocket serverSocket;
    private Hashtable<Integer, MapOutputReader> readers;

    public MapOutputReadServer(int port, Hashtable<Integer, MapOutputReader> readers) throws IOException{
        serverSocket = new ServerSocket(port);
        this.readers = readers;
        ///serverSocket.setSoTimeout(50000);
    }

    @Override
    public void run(){
        while (true){
            try {
                System.out.println("Waiting for conncetion...");
                System.out.println("Port: " + serverSocket.getLocalPort());

                Socket server = serverSocket.accept();
                System.out.println("Connected to: " + server.getRemoteSocketAddress());

                DataInputStream in = new DataInputStream(server.getInputStream());
                String outputName = in.readUTF();
                int mapperId = in.readInt();
                System.out.println(outputName);
                ArrayList<IndexRecord> indexList = getIndexList(in);

                MapOutputReader reader = new MapOutputReader(outputName, indexList);
                System.out.println("Getting MapOutputReader: " + mapperId);
                readers.put(mapperId, reader);
                /*
                // Read a 64*1024 bytebuffer to buf
                ByteBuffer buf = ByteBuffer.allocate(reader.BLOCK_SIZE);
                Future<Integer> len = reader.getBlockFuture(0, buf);

                System.out.println(len.get());
                buf.flip();
                for (int i = 0; i < buf.limit(); i++){
                    System.out.print((char)buf.get());
                }*/
                in.close();
                server.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private ArrayList<IndexRecord> getIndexList(DataInputStream in) throws IOException {

        ArrayList<IndexRecord> indexList = new ArrayList<>();

        int partitions = in.readInt();

        for (int i = 0; i < partitions; i++){
            long startOffset = in.readLong();
            long rawLength = in.readLong();
            long partLength = in.readLong();
            IndexRecord ir = new IndexRecord(startOffset, rawLength, partLength);
            indexList.add(ir);
        }
        return indexList;
    }
}
