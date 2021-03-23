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
    HashMap<Metadata.Edge, Long> timestamp = new HashMap<>();
    List<Integer> replicas = new ArrayList<>();
    List<HashSet<Integer>> replicaKeys = new ArrayList<>();
    List<HashSet<Metadata.Edge>> replicaEdges = new ArrayList<>();
    HashMap<Integer, Integer> replicaMap = new HashMap<>();
    Random rand = new Random();
    boolean request_timestamp = false;

    public ReplicaCentricClient(ConfigReader cnfReader) {
        super(cnfReader);
        HashMap<String, List<String>> protocolProperties = cnfReader.getProtocolProperties();
        clientId = new Integer(protocolProperties.get("client_id").get(0));
        numOfBuckets = new Integer(protocolProperties.get("num_of_buckets").get(0));
        // To locate the replica ID easier, use a hash map to store the ID and the index in replicas.
        int idx = 0;
        for (String r: protocolProperties.get("client" + clientId)) {
            replicas.add(new Integer(r));
            replicaMap.put(new Integer(r), idx);
            idx++;
        }
    }

    @Override
    public boolean put(String key, byte[] value) {
        try {
            if (!request_timestamp) {
                request_timestamp = requestTimestamps();
            }
            //key = String.valueOf((Utils.getMd5HashLong(key) % 5));
            int serverId = clientId;
            // If this key belongs to the local replica, directly insert to the local replica.
            // If not, randomly choose a destination replica containing this key.
            if (!localReplicaContains(key)) {
                serverId = randomReplica(key);
            }
            if (serverId > 0) {
                Metadata.PutMessage pm = Metadata.PutMessage.newBuilder().setKey(key).setValue(
                        Metadata.Record.newBuilder().setValue(ByteString.copyFrom(value)).
                                build()).addAllTimestamps(getTimestampList(serverId)).build();
                Metadata.ClientMessage cm = Metadata.ClientMessage.newBuilder().setPutMessage(pm).build();
                if (sendToServer(String.valueOf(serverId), cm) == ServerConnector.NetworkStatus.FAILURE)
                    return false;
                Metadata.ClientReply cr = readFromServer(String.valueOf(serverId));
                if (cr != null && cr.getPutReply().getStatus()) {
                    // Update the joint timestamp.
                    merge(cr.getPutReply().getTimestampsList());
                    // Force the local replica's timestamp is no smaller than this client's timestamp.
                    if (serverId != clientId) {
                        if (!updateLocalReplica()) {
                            protocolLOGGER.severe("Local replica could not be updated.");
                            return false;
                        }
                    }
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

    private boolean localReplicaContains(String key) {
        // Map the key into the corresponding bucket.
        int bucket = -1;
        try {
            bucket = findBucket(key);
        } catch (NoSuchAlgorithmException e) {
            protocolLOGGER.severe("Problem finding bucket for key " + key);
        }
        return replicaKeys.get(replicaMap.get(clientId)).contains(bucket);
    }

    private boolean updateLocalReplica() {
        Metadata.ClientMessage cm = Metadata.ClientMessage.newBuilder().setUpdateTMessage(Metadata.UpdateTMessage.
                newBuilder().addAllTimestamps(getTimestampList(clientId)).build()).build();
        if (sendToServer(String.valueOf(clientId), cm) == ServerConnector.NetworkStatus.SUCCESS) {
            Metadata.ClientReply cr = readFromServer(String.valueOf(clientId));
            if (cr != null && cr.getUpdateTReply().getStatus()) {
                return true;
            }
        }
        return false;
    }

    private boolean requestTimestamps() {
        Metadata.ClientMessage cm = Metadata.ClientMessage.newBuilder().setTMessage(Metadata.TimestampMessage.
                newBuilder().setRequestTimestamp(true).build()).build();
        for (Integer r: replicas) {
            if (sendToServer(String.valueOf(r), cm) == ServerConnector.NetworkStatus.SUCCESS) {
                Metadata.ClientReply cr = readFromServer(String.valueOf(r));
                if (cr != null && cr.hasTReply() && !cr.getTReply().getTimestampsList().isEmpty()) {
                    HashSet<Metadata.Edge> tempEdges = new HashSet();
                    for (Metadata.Dependency dep: cr.getTReply().getTimestampsList()) {
                        tempEdges.add(dep.getEdge());
                        if (!timestamp.containsKey(dep.getEdge()) || timestamp.get(dep.getEdge()) < dep.getVersion()) {
                            timestamp.put(dep.getEdge(), dep.getVersion());
                        }
                    }
                    replicaKeys.add(new HashSet<>(cr.getTReply().getKeysList()));
                    replicaEdges.add(tempEdges);
                } else if (cr.hasTReply()) {
                    replicaKeys.add(new HashSet<>(cr.getTReply().getKeysList()));
                    replicaEdges.add(new HashSet<>());
                } else {
                    replicaKeys.add(new HashSet<>());
                    replicaEdges.add(new HashSet<>());
                }
            } else {
                protocolLOGGER.severe("Cannot get the timestamp from replica" + r);
                return false;
            }
        }
        return true;
    }

    private List<Metadata.Dependency> getTimestampList(int serverId) {
        List<Metadata.Dependency> temp_timestamps = new ArrayList<>();
        int idx = replicaMap.get(serverId);
        for (Metadata.Edge e: replicaEdges.get(idx)) {
            Metadata.Dependency dep = Metadata.Dependency.newBuilder().setEdge(e).setVersion(timestamp.get(e)).build();
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
                return replicas.get(i % replicas.size());
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
            int serverId = clientId;
            //key = String.valueOf((Utils.getMd5HashLong(key) % 5));
            // If this key belongs to the local replica, directly insert to the local replica.
            // If not, randomly choose a destination replica containing this key.
            if (!localReplicaContains(key)) {
                serverId = randomReplica(key);
            }
            if (serverId > 0) {
                Metadata.GetMessage gm = Metadata.GetMessage.newBuilder().setKey(key).addAllTimestamps(getTimestampList(serverId)).build();
                Metadata.ClientMessage cm = Metadata.ClientMessage.newBuilder().setGetMessage(gm).build();
                if (sendToServer(String.valueOf(serverId), cm) == ServerConnector.NetworkStatus.FAILURE)
                    return null;
                Metadata.ClientReply cr = readFromServer(String.valueOf(serverId));
                if (cr != null && cr.getGetReply().getStatus()) {
                    // Update the client's timestamp.
                    merge(cr.getGetReply().getTimestampsList());
                    // Force the local replica's timestamp is no smaller than this client's timestamp.
                    if (serverId != clientId) {
                        if (!updateLocalReplica()) {
                            protocolLOGGER.severe("Local replica could not be updated.");
                            return null;
                        }
                    }
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
