package edu.msu.cse.replicacentric.server;

import edu.msu.cse.dkvf.ClientMessageAgent;
import edu.msu.cse.dkvf.DKVFServer;
import edu.msu.cse.dkvf.Storage;
import edu.msu.cse.dkvf.config.ConfigReader;
import edu.msu.cse.dkvf.metadata.Metadata;
import javafx.util.Pair;

import java.security.NoSuchAlgorithmException;
import java.text.MessageFormat;
import java.util.*;
import java.util.stream.Collectors;

import static edu.msu.cse.dkvf.metadata.Metadata.*;

public class ReplicaCentricServer extends DKVFServer {

    class pendings implements Runnable {
        //private Thread t;

        public void run() {
            Iterator<ReplicateMessage> iterator = pendingReplicateMessages.iterator();
            while (iterator.hasNext()){
                ReplicateMessage rm = iterator.next();
                boolean updateNow = false, flag = true;
                Edge edge = Edge.newBuilder().setVertex1(rm.getServerId()).setVertex2(serverId).build();
                // This is used to store the edges ∈ E_serverId ∩ E_rm.getServerId().
                List<Pair<Edge, Integer>> intersectEdges = new ArrayList<>();

                for (Dependency dep: rm.getTimestampsList()) {
                    if (dep.getEdge().getVertex2() == serverId) {
                        intersectEdges.add(new Pair<>(dep.getEdge(), (int) dep.getVersion()));
                    }
                    if (dep.getEdge() == edge) {
                        if (timestamp.get(edge) < dep.getVersion() - 1) {
                            // This replicate message needs to wait for this server's update later.
                            break;
                        } else if (timestamp.get(edge) >= dep.getVersion()) {
                            // This server is newer than this replicate message.
                            synchronized (pendingReplicateMessages) {
                                iterator.remove();
                            }
                            break;
                        } else {
                            updateNow = true;
                        }
                    } else if (dep.getEdge().getVertex2() == serverId && timestamp.get(dep.getEdge()) < dep.getVersion()) {
                        flag = false;
                        break;
                    }
                }
                if (updateNow && flag) {
                    // Write the value of this replicate message to local storage.
                    Storage.StorageStatus ss = insert(rm.getKey(), rm.getRec());
                    if (ss == Storage.StorageStatus.SUCCESS) {
                        // update the timestamp of current server.
                        for (Pair<Edge, Integer> e: intersectEdges) {
                            timestamp.put(e.getKey(), Math.max(timestamp.get(e.getKey()), e.getValue()));
                        }
                        protocolLOGGER.finer(MessageFormat.format("Replicate Message {0} is handled and removed", rm));
                        // Remove this completed replicate message.
                        synchronized (pendingReplicateMessages) {
                            iterator.remove();
                        }
                    }
                }
                intersectEdges.clear();
            }
        }
    }

    int serverId;
    int numOfServers;
    int numOfBuckets;
    // Use hash map to store the timestamps of tracked edges instead of list.
    // Let the searching become more efficient.
    //List<Metadata.Dependency> timestamp;
    HashMap<Edge, Integer> timestamp;
    // Adjacent Matrix to represent the shared graph.
    HashSet<Integer>[][] AdMatrix;
    List<ReplicateMessage> pendingReplicateMessages;
    List<HashSet<Integer>> sharedGraph;

    /**
     * Constructor for DKVFServer
     *
     * @param cnfReader The configuration reader
     */
    public ReplicaCentricServer(ConfigReader cnfReader) {
        super(cnfReader);

        HashMap<String, List<String>> protocolProperties = cnfReader.getProtocolProperties();

        serverId = new Integer(protocolProperties.get("server_id").get(0));

        // Assume the ID of servers are 1, 2, ..., numOfServers.
        numOfServers = new Integer(protocolProperties.get("num_of_servers").get(0));
        numOfBuckets = new Integer(protocolProperties.get("num_of_buckets").get(0));
        timestamp = new HashMap<>();
        AdMatrix = new HashSet[numOfServers][numOfServers];
        pendingReplicateMessages = new ArrayList<>();
        sharedGraph = new ArrayList<>();

        for(int i = 1; i <= numOfServers; i++) {
            HashSet<Integer> keys = new HashSet<>();
            if (protocolProperties.get("server" + i) != null) {
                for (String key: protocolProperties.get("server" + i)) {
                    keys.add(new Integer(key));
                }
            }
            sharedGraph.add(keys);
        }



        generateDependencies();

        // Run the parallel thread to check pending replicate messages.
        pendings p = new pendings();
        Thread t = new Thread(p);
        t.start();
    }

    // This function generates the timestamp to track of this server according to the sharedGraph.
    private void generateDependencies() {
        // Generate the adjacent matrix according to each replica's key range.
        for(int i = 0; i < numOfServers; i++) {
            for(int j = i+1; j < numOfServers; j++) {
                AdMatrix[i][j] = intersection(sharedGraph.get(i), sharedGraph.get(j));
            }
        }

        for(int i = 0; i < numOfServers; i++) {
            for(int j = 0; j < i; j++) {
                AdMatrix[i][j] = AdMatrix[j][i];
            }
        }

        for (int i = 0; i < numOfServers; i++) {
            AdMatrix[i][i] = new HashSet<>();
        }

        // Firstly, the tracked edges include neighboring edges.
        // These edges represent there are shared keys between two vertices.
        for (int i = 0; i < numOfServers; i++) {
            // Both directions should be tracked.
            if(!AdMatrix[serverId-1][i].isEmpty()) {
                timestamp.put(Edge.newBuilder().setVertex1(serverId).setVertex2(i+1).build(), 0);
                timestamp.put(Edge.newBuilder().setVertex1(i+1).setVertex2(serverId).build(), 0);
            }
        }

        // Secondly, the tracked edges include edge ejk in (i, ejk)-loop.
        loop();
    }

    // Detect loops containing serverId by using DFS.
    private void loop() {
        // Use DFS to identify each loop containing serverId.
        // Current visiting path.
        List<Integer> path = new ArrayList<>();
        // Store the visited servers.
        boolean[] visited = new boolean[numOfServers];
        Arrays.fill(visited, Boolean.FALSE);

        // the start point of the loop is current server.
        path.add(serverId);
        visited[serverId-1] = true;
        DFSHelper(path, visited, serverId);
    }

    private void DFSHelper(List<Integer> path, boolean[] visited, int Id) {
        for (int i = 1; i <= numOfServers; i++) {
            // There is an edge between current server and server i.
            if(!AdMatrix[i-1][Id-1].isEmpty()) {
                if (i == serverId) {
                    // A loop is formed. Check whether each edge in the loop should be tracked or not.
                    if(path.size() < 3)
                        continue;
                    path.add(serverId);
                    checkEdges(path);
                } else if (visited[i-1]) {
                    // Current loop doesn't contain serverId. Ignore;
                    continue;
                } else {
                    // Continue forming the loop.
                    path.add(i);
                    visited[i-1] = true;
                    DFSHelper(path, visited, i);
                    path.remove(path.size()-1);
                    visited[i-1] = false;
                }
            }
        }
    }

    // And check whether each edge should be tracked or not.
    // For loop (i, i+1, ..., k-1, k, k+1, k+2, k+3, ... i-1, i), edge(k+1->k) should be tracked iff
    // 1. server k+1 and server k share at least one distinct key from server i+1 to k-1.
    // 2. server k+1 and server k+2 share at least one distinct key from server i+1 to k-1.
    // 3. server m and server m+1 (k+2<=m<=i-1) share at least one distinct key from server i+1 to k.
    private void checkEdges(List<Integer> path) {
        // Firstly, check edge path[k+1]->path[k].
        int k = 1;

        HashSet<Integer> union = new HashSet<>();

        while (k + 2 < path.size()) {
            // Edge path[k+1]->path[k].
            // satisfy the first and the second condition.
            if (!timestamp.containsKey(Metadata.Edge.newBuilder().setVertex1(path.get(k+1)).setVertex2(path.get(k)).build()) &&
                    distinct(AdMatrix[path.get(k)-1][path.get(k+1)-1], union) && distinct(AdMatrix[path.get(k+1)-1][path.get(k+2)-1], union)) {
                union.addAll(sharedGraph.get(path.get(k)-1));
                int j = k + 2;
                for (; j <= path.size()-2; j++) {
                    // The edges in this loop between path[k+1]->path[k] and path[j+1]->path[j] are not supposed to be tracked.
                    if (!distinct(AdMatrix[path.get(j)-1][path.get(j+1)-1], union)) {
                        // Directly jump to check edge path[j+2]->path[j+1].
                        // The union should contain all keys of server path[i+1] to server path[j].
                        for (int l = k+1; l <= j; ++l) {
                            union.addAll(sharedGraph.get(path.get(l)-1));
                        }
                        k = j;
                        break;
                    }
                }
                // satisfy the third condition.
                if (j == path.size() - 1) {
                    //timestamp.add(Metadata.Dependency.newBuilder().setVertex1(path.get(k+1)).setVertex2(path.get(k)).setVersion(0).build());
                    timestamp.put(Metadata.Edge.newBuilder().setVertex1(path.get(k+1)).setVertex2(path.get(k)).build(), 0);
                }
            } else {
                union.addAll(sharedGraph.get(path.get(k)-1));
            }
            ++k;
        }
    }

    // whether set1 - set2 != empty set?
    private boolean distinct(HashSet<Integer> set1, HashSet<Integer> set2) {
        for (int i: set1) {
            if (!set2.contains(i))
                return true;
        }
        return false;
    }

    // This function is used to get the shared keys of two replicas.
    private HashSet<Integer> intersection(HashSet<Integer> s1, HashSet<Integer> s2) {
        HashSet<Integer> result = new HashSet<>();
        if (s1.size() <= s2.size()) {
            for(Integer i: s1) {
                if(s2.contains(i)) {
                    result.add(i);
                }
            }
        } else {
            for (Integer i: s2) {
                if(s1.contains(i)) {
                    result.add(i);
                }
            }
        }
        return result;
    }

    @Override
    public void handleClientMessage(ClientMessageAgent cma) throws NoSuchAlgorithmException {
        if (cma.getClientMessage().hasGetMessage()) {
            handleGetMessage(cma);
        } else if(cma.getClientMessage().hasPutMessage()) {
            handlePutMessage(cma);
        }
    }

    private void handlePutMessage(ClientMessageAgent cma) throws NoSuchAlgorithmException {
        PutMessage pm = cma.getClientMessage().getPutMessage();

        //Firstly, insert the value into the local server.
        //Currently, we only allow the specified keys to insert in current server.
        Storage.StorageStatus ss = Storage.StorageStatus.FAILURE;
        if (sharedGraph.get(serverId-1).contains(findBucket(pm.getKey()))) {
            ss = insert(pm.getKey(), pm.getValue());
        }

        // Initialize the client reply;
        ClientReply cr = null;
        if (ss == Storage.StorageStatus.SUCCESS) {
            cr = ClientReply.newBuilder().setPutReply(PutReply.newBuilder().setStatus(true).build()).build();
        } else {
            cr = ClientReply.newBuilder().setPutReply(PutReply.newBuilder().setStatus(false).build()).build();
            cma.sendReply(cr);
            return;
        }

        // Secondly, update timestamp.
        for (Map.Entry<Edge, Integer> dep: timestamp.entrySet()) {
            int v1 = dep.getKey().getVertex1(), v2 = dep.getKey().getVertex2();

            if (v1 == serverId && AdMatrix[v1-1][v2-1].contains(findBucket(pm.getKey()))) {
                timestamp.put(dep.getKey(), dep.getValue() + 1);
            }
        }

        // Thirdly, send replicate message to other replicas which also contain key.
        ServerMessage sm = ServerMessage.newBuilder().setReplicateMessage(
                ReplicateMessage.newBuilder().setKey(pm.getKey()).setRec(pm.getValue()).setServerId(serverId).addAllTimestamps((Iterable<? extends Dependency>) timestamp).build()).build();
        for (int i = 0; i < numOfServers; i++) {
            if(AdMatrix[serverId-1][i].contains(findBucket(pm.getKey()))) {
                protocolLOGGER.finer(MessageFormat.format("send replicate message to server{0}", i+1));
                sendToServerViaChannel(Integer.toString(i+1), sm);
            }
        }

        // Return reply to the client.
        cma.sendReply(cr);
    }

    private Integer findBucket(String key) throws NoSuchAlgorithmException {
        long hash = edu.msu.cse.dkvf.Utils.getMd5HashLong(key);
        return (int) (hash % numOfBuckets);
    }

    private void handleGetMessage(ClientMessageAgent cma) {
        // Need to verify whether DKVF supports single version value.
        GetMessage gm = cma.getClientMessage().getGetMessage();
        List<Record> result = new ArrayList<>();
        Storage.StorageStatus ss = read(gm.getKey(), (Record rec) -> {
            return true;
        }, result);
        ClientReply cr = null;

        if (ss == Storage.StorageStatus.SUCCESS) {
            Record rec = result.get(0);
            cr = ClientReply.newBuilder().setGetReply(GetReply.newBuilder().setStatus(true).setRecord(rec).build()).build();
        } else {
            cr = ClientReply.newBuilder().setGetReply(GetReply.newBuilder().setStatus(false).build()).build();
        }
        cma.sendReply(cr);
    }

    @Override
    public void handleServerMessage(ServerMessage sm) {
        if (sm.hasReplicateMessage()) {
            handleReplicateMesssage(sm);
        }
    }

    private void handleReplicateMesssage(ServerMessage sm) {
        protocolLOGGER.finer(MessageFormat.format("Received replicate message: {0}", sm.toString()));
        ReplicateMessage rm = sm.getReplicateMessage();
        protocolLOGGER.finer(MessageFormat.format("Replicate Message {0} is received", rm.toString()));

        // Firstly, add replicate message to pendings.
        synchronized (pendingReplicateMessages) {
            pendingReplicateMessages.add(rm);
        }
    }



}

