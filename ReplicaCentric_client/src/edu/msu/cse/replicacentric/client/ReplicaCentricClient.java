package edu.msu.cse.replicacentric.client;

import com.google.protobuf.ByteString;
import edu.msu.cse.dkvf.DKVFClient;
import edu.msu.cse.dkvf.ServerConnector;
import edu.msu.cse.dkvf.Utils;
import edu.msu.cse.dkvf.config.ConfigReader;
import edu.msu.cse.dkvf.metadata.Metadata;

import java.util.HashMap;
import java.util.List;

public class ReplicaCentricClient extends DKVFClient {

    int serverId;

    public ReplicaCentricClient(ConfigReader cnfReader) {
        super(cnfReader);
        HashMap<String, List<String>> protocolProperties = cnfReader.getProtocolProperties();
        serverId = new Integer(protocolProperties.get("server_id").get(0));
    }

    @Override
    public boolean put(String key, byte[] value) {
        try {
            Metadata.PutMessage pm = Metadata.PutMessage.newBuilder().setKey(key).setValue(
                    Metadata.Record.newBuilder().setValue(ByteString.copyFrom(value)).build()).build();
            Metadata.ClientMessage cm = Metadata.ClientMessage.newBuilder().setPutMessage(pm).build();
            if (sendToServer(String.valueOf(serverId), cm) == ServerConnector.NetworkStatus.FAILURE)
                return false;
            Metadata.ClientReply cr = readFromServer(String.valueOf(serverId));
            if (cr != null && cr.getPutReply().getStatus()) {
                return true;
            } else {
                protocolLOGGER.severe("Server could not put the key= " + key);
                return false;
            }
        } catch (Exception e) {
            protocolLOGGER.severe(Utils.exceptionLogMessge("Failed to put due to exception", e));
            return false;
        }
    }

    @Override
    public byte[] get(String key) {
        try {
            Metadata.GetMessage gm = Metadata.GetMessage.newBuilder().setKey(key).build();
            Metadata.ClientMessage cm = Metadata.ClientMessage.newBuilder().setGetMessage(gm).build();
            if (sendToServer(String.valueOf(serverId), cm) == ServerConnector.NetworkStatus.FAILURE)
                return null;
            Metadata.ClientReply cr = readFromServer(String.valueOf(serverId));
            if (cr != null && cr.getGetReply().getStatus()) {
                return cr.getGetReply().getRecord().getValue().toByteArray();
            } else {
                protocolLOGGER.severe("Server could not get the key= " + key);
                return null;
            }
        } catch (Exception e) {
            protocolLOGGER.severe(Utils.exceptionLogMessge("Failed to get due to exception", e));
            return null;
        }
    }
}
