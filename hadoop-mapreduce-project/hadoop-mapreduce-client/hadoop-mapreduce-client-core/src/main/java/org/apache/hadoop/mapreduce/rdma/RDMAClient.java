package org.apache.hadoop.mapreduce.rdma;

import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.util.DiSNILogger;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;


public class RDMAClient {
    private RdmaActiveEndpointGroup<ClientEndpoint> endpointGroup;


    public RDMAClient() throws IOException {
        ReducerClientEndpointFactory factory = new ReducerClientEndpointFactory();
        endpointGroup = factory.getEndpointGroup();
    }

    public RdmaDataInputStream createRdmaStream(String host, int port) throws Exception {
        InetAddress ipAddress = InetAddress.getByName(host);
        InetSocketAddress address = new InetSocketAddress(ipAddress, port);
        return new RdmaDataInputStream(endpointGroup, address);
    }


    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        RDMAClient simpleClient = new RDMAClient();

        /*
        CmdLineCommon cmdLine = new CmdLineCommon("rdma.Client");

        try {
            cmdLine.parse(args);
        } catch (ParseException e) {
            cmdLine.printHelp();
            System.exit(-1);
        }
        String host = cmdLine.getIp();
        int port = cmdLine.getPort();
         */

        String host = "";
        int port = -1;
        RdmaDataInputStream rdmaStream = simpleClient.createRdmaStream(host, port);
        int testing_mapperId = 0;
        int testing_reducerId = 0;

        for (int i = 0; i < 50; i++) {
            byte[] byteArray = new byte[RdmaConfigs.LOAD_SIZE];
            DiSNILogger.getLogger().info("Get mapperId " + testing_mapperId + " for reducer: " + testing_reducerId);
            rdmaStream.prepareInfo(testing_mapperId++, testing_reducerId++);
            int bytesWritten = rdmaStream.read(byteArray, 0, 100);
            DiSNILogger.getLogger().info("ByteArray" + i + ": " + new String(byteArray, 0, bytesWritten));
        }
        rdmaStream.closeEndpoint();
        simpleClient.closeEndpointGroup();
    }

    public void closeEndpointGroup() throws IOException, InterruptedException {
        this.endpointGroup.close();
    }

}