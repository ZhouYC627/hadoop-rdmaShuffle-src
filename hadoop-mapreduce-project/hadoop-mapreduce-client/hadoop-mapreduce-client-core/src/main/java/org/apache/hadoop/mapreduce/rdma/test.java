package org.apache.hadoop.mapreduce.rdma;
import java.nio.ByteBuffer;



public class test {

    public static void main(String[] args) {
        ByteBuffer dataBuf = ByteBuffer.allocateDirect(RdmaConfigs.LOAD_SIZE);

        String msg = "This is Required data of mapperId " + 1 + " for reducerId " + 100;
        byte[] msgArr = msg.getBytes();
        dataBuf.put(msgArr);
        System.out.println(msgArr.length);

        dataBuf.clear();
        byte[] byteArray = new byte[RdmaConfigs.LOAD_SIZE];

        int length = 0;
        while (length < dataBuf.limit()) {
            byte b = dataBuf.get();
            if (b == 0) {
                break;
            }
            byteArray[length++] = b;
        }

        System.out.println(new String(byteArray, 0, length));


        String msg2 = "Fuck Rdma";
        byte[] msgArr2 = msg2.getBytes();
        dataBuf.clear();
        dataBuf.put(msgArr2);
        System.out.println(msgArr2.length);

        dataBuf.clear();
        byte[] byteArray2 = new byte[RdmaConfigs.LOAD_SIZE];

        int length2 = 0;
        while (length2 < dataBuf.limit()) {
            byte b = dataBuf.get();
            if (b == 0) {
                break;
            }
            byteArray2[length2++] = b;
        }

        System.out.println(new String(byteArray2, 0, length2));
    }
}
