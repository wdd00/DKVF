package edu.msu.cse.replicacentric.client;

import com.google.protobuf.ByteString;
import edu.msu.cse.dkvf.DKVFClient;
import edu.msu.cse.dkvf.ServerConnector;
import edu.msu.cse.dkvf.Utils;
import edu.msu.cse.dkvf.config.ConfigReader;
import edu.msu.cse.dkvf.metadata.Metadata;

import java.security.NoSuchAlgorithmException;
import java.util.*;

public class ReplicaCentricClient extends DKVFClient {

    int clientId;
    int numOfBuckets;
    int OpIdx = 1;
    HashMap<Metadata.Edge, Long> timestamp = new HashMap<>();
    List<Integer> replicas = new ArrayList<>();
    List<HashSet<Integer>> replicaKeys = new ArrayList<>();
    Random rand = new Random();
    boolean request_timestamp = false;

    public ReplicaCentricClient(ConfigReader cnfReader) {
        super(cnfReader);
        HashMap<String, List<String>> protocolProperties = cnfReader.getProtocolProperties();
        clientId = new Integer(protocolProperties.get("client_id").get(0));
        numOfBuckets = new Integer(protocolProperties.get("num_of_buckets").get(0));
        for (String r: protocolProperties.get("client" + clientId)) {
            replicas.add(new Integer(r));
        }
        protocolLOGGER.info("client's construction at " + System.currentTimeMillis());
    }

    @Override
    public boolean put(String key, byte[] value) {
        try {
            if (!request_timestamp) {
                request_timestamp = requestTimestamps();
            }
            // Convert the randomly generated keys into one of the 100 keys.
            key = String.valueOf((Utils.getMd5HashLong(key) % 20));
            long startTime = System.currentTimeMillis();
            Metadata.PutMessage pm = Metadata.PutMessage.newBuilder().setKey(key).setValue(
                    Metadata.Record.newBuilder().setValue(ByteString.copyFrom(value)).setClientId(clientId).
                            setSourceOpIdx(OpIdx).build()).addAllTimestamps(getTimestampList()).build();
            Metadata.ClientMessage cm = Metadata.ClientMessage.newBuilder().setPutMessage(pm).build();
            int serverId = randomReplica(key);
            if (serverId > 0) {
                if (sendToServer(String.valueOf(serverId), cm) == ServerConnector.NetworkStatus.FAILURE)
                    return false;
                Metadata.ClientReply cr = readFromServer(String.valueOf(serverId));
                if (cr != null && cr.getPutReply().getStatus()) {
                    // Update the joint timestamp.
                    merge(cr.getPutReply().getTimestampsList());
                    long endTime = System.currentTimeMillis();
                    OpIdx++;
                    protocolLOGGER.info("PUT " + key + " " + pm.getValue() + "starts " + startTime + " ends " + endTime);
                    return true;
                } else {
                    protocolLOGGER.severe("Server could not put the key= " + key);
                    return false;
                }
            } else {
                protocolLOGGER.severe("Failed to find a destination replica.");
                return false;
            }
        } catch (Exception e) {
            protocolLOGGER.severe(Utils.exceptionLogMessge("Failed to put due to exception", e));
            return false;
        }
    }

    private boolean requestTimestamps() {
        Metadata.ClientMessage cm = Metadata.ClientMessage.newBuilder().setTMessage(Metadata.TimestampMessage.
                newBuilder().setRequestTimestamp(true).build()).build();
        for (Integer r: replicas) {
            if (sendToServer(String.valueOf(r), cm) == ServerConnector.NetworkStatus.SUCCESS) {
                Metadata.ClientReply cr = readFromServer(String.valueOf(r));
                if (cr.hasTReply() && !cr.getTReply().getTimestampsList().isEmpty()) {
                    for (Metadata.Dependency dep: cr.getTReply().getTimestampsList()) {
                        if (!timestamp.containsKey(dep.getEdge()) || timestamp.get(dep.getEdge()) < dep.getVersion()) {
                            timestamp.put(dep.getEdge(), dep.getVersion());
                        }
                    }
                    replicaKeys.add(new HashSet<>(cr.getTReply().getKeysList()));
                } else if (cr.hasTReply()) {
                    replicaKeys.add(new HashSet<>(cr.getTReply().getKeysList()));
                } else {
                    replicaKeys.add(new HashSet<>());
                }
            } else {
                protocolLOGGER.severe("Cannot get the timestamp from replica" + r);
                return false;
            }
        }
        return true;
    }

    private List<Metadata.Dependency> getTimestampList() {
        List<Metadata.Dependency> temp_timestamps = new ArrayList<>();
        for (Map.Entry<Metadata.Edge, Long> entry: timestamp.entrySet()) {
            Metadata.Dependency dep = Metadata.Dependency.newBuilder().setEdge(entry.getKey()).setVersion(entry.getValue()).build();
            temp_timestamps.add(dep);
        }
        return temp_timestamps;
    }

    // Merge the joint timestamp and the received timestamp.
    private void merge(List<Metadata.Dependency> ReplicaTimestamp) {
        for (Metadata.Dependency dep: ReplicaTimestamp) {
                if (dep.getVersion() > timestamp.get(dep.getEdge())) {
                    timestamp.put(dep.getEdge(), dep.getVersion());
                }
        }
    }

    private Integer randomReplica(String key) {
        // Map the key into the corresponding bucket.
        int bucket = -1;
        try {
            bucket = findBucket(key);
        } catch (NoSuchAlgorithmException e) {
            protocolLOGGER.severe("Problem finding bucket for key " + key);
        }
        // Randomly choose a destination replica.
        int randId = rand.nextInt(replicas.size());
        for (int i = randId; i < randId + replicas.size(); i++) {
            if (replicaKeys.get(i % replicas.size()).contains(bucket)) {
                return (i % replicas.size()) + 1;
            }
        }
        return -1;
    }

    private Integer findBucket(String key) throws NoSuchAlgorithmException {
        long hash = edu.msu.cse.dkvf.Utils.getMd5HashLong(key);
        return (int) (hash % numOfBuckets);
    }

    @Override
    public byte[] get(String key) {
        try {
            if (!request_timestamp) {
                request_timestamp = requestTimestamps();
            }
            // Convert the randomly generated keys into one of the 100 keys.
            key = String.valueOf((Utils.getMd5HashLong(key) % 20));
            long startTime = System.currentTimeMillis();
            Metadata.GetMessage gm = Metadata.GetMessage.newBuilder().setKey(key).addAllTimestamps(getTimestampList()).build();
            Metadata.ClientMessage cm = Metadata.ClientMessage.newBuilder().setGetMessage(gm).build();
            int serverId = randomReplica(key);
            if (serverId > 0) {
                if (sendToServer(String.valueOf(serverId), cm) == ServerConnector.NetworkStatus.FAILURE)
                    return null;
                Metadata.ClientReply cr = readFromServer(String.valueOf(serverId));
                if (cr != null && cr.getGetReply().getStatus()) {
                    // Update the client's timestamp.
                    merge(cr.getGetReply().getTimestampsList());
                    long endTime = System.currentTimeMillis();
                    protocolLOGGER.info("GET " + key + " " + cr.getGetReply().getRecord() + "starts " + startTime + " ends " + endTime);
                    return cr.getGetReply().getRecord().getValue().toByteArray();
                } else {
                    protocolLOGGER.severe("Server could not get the key= " + key);
                    return null;
                }
            } else {
                protocolLOGGER.severe("Failed to find a destination replica.");
                return null;
            }
        } catch (Exception e) {
            protocolLOGGER.severe(Utils.exceptionLogMessge("Failed to get due to exception", e));
            return null;
        }
    }
}
