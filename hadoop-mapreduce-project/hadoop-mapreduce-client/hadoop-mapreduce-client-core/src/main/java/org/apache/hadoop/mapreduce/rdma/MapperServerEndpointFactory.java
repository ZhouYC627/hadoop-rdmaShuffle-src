package org.apache.hadoop.mapreduce.rdma;

import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.RdmaEndpointFactory;
import com.ibm.disni.verbs.RdmaCmId;

import java.io.IOException;

/** Parameters in ActiveEndpointGroup
 * Polling (used in creating CQ): Polling the CQ for completion is getting the details about a WR (Send or Receive)
 * that was posted. A completion channel (CC) is used to allow the user to be notified that a new CQE is on the CQ.
 * If true, then user polls the CQ at regular intervals. CC is not necessary.
 * If false, then CC is required.
 * maxWR: maximum work request at this group
 * maxSge: maximum sge at this group
 * cq_Size: size of this CQ
 */
public class MapperServerEndpointFactory implements RdmaEndpointFactory<MapperEndpoint> {
    private final RdmaActiveEndpointGroup<MapperEndpoint> endpointGroup;

    public MapperServerEndpointFactory() throws IOException {
        endpointGroup = new RdmaActiveEndpointGroup<>(1000, false, 128, 4, 128);
        endpointGroup.init(this);
    }

    @Override
    public MapperEndpoint createEndpoint(RdmaCmId id, boolean serverSide) throws IOException {
        return new MapperEndpoint(endpointGroup, id, serverSide);
    }

    public RdmaActiveEndpointGroup<MapperEndpoint> getEndpointGroup() {
        return this.endpointGroup;
    }
}
