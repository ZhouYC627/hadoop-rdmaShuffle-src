package org.apache.hadoop.mapreduce.rdma;

import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.RdmaEndpointFactory;
import com.ibm.disni.verbs.RdmaCmId;

import java.io.IOException;

public class ReducerClientEndpointFactory implements RdmaEndpointFactory<ClientEndpoint> {
    private final RdmaActiveEndpointGroup<ClientEndpoint> endpointGroup;

    public ReducerClientEndpointFactory() throws IOException{
        endpointGroup = new RdmaActiveEndpointGroup<>(1000, false, 128, 4, 128);
        endpointGroup.init(this);
    }

    @Override
    public ClientEndpoint createEndpoint(RdmaCmId id, boolean serverSide) throws IOException {
        return new ClientEndpoint(endpointGroup, id, serverSide);
    }

    public void close() throws IOException, InterruptedException {
        endpointGroup.close();
    }

    public RdmaActiveEndpointGroup<ClientEndpoint> getEndpointGroup() {
        return this.endpointGroup;
    }
}
