package edu.msu.cse.replicacentric.server;

import edu.msu.cse.dkvf.ClientMessageAgent;
import edu.msu.cse.dkvf.DKVFServer;
import edu.msu.cse.dkvf.ServerConnector;
import edu.msu.cse.dkvf.Storage;
import edu.msu.cse.dkvf.config.ConfigReader;
import edu.msu.cse.dkvf.metadata.Metadata;
import javafx.util.Pair;

import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static edu.msu.cse.dkvf.metadata.Metadata.*;

public class ReplicaCentricServer extends DKVFServer {
    int serverId;
    int numOfServers;
    int numOfBuckets;
    // Use hash map to store the timestamps of tracked edges instead of list.
    // Let the searching become more efficient.
    //List<Metadata.Dependency> timestamp;
    HashMap<Edge, Long> timestamp;
    // Adjacent Matrix to represent the shared graph.
    HashSet<Integer>[][] AdMatrix;
    List<ReplicateMessage> pendingReplicateMessages;
    List<ClientMessageAgent> pendingClientMessages;
    List<HashSet<Integer>> sharedGraph;
    // This set is used to store the edges constructed by one same client.
    HashSet<Edge> augmentedEdges;
    // Timestamp lock
    ReadWriteLock lock = new ReentrantReadWriteLock();
    Lock writeLock = lock.writeLock();
    Lock readLock = lock.readLock();
    // pending client message lock
    ReadWriteLock clientLock = new ReentrantReadWriteLock();
    Lock clientWriteLock = clientLock.writeLock();
    Lock clientReadLock = clientLock.readLock();

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
        pendingClientMessages = new ArrayList<>();
        sharedGraph = new ArrayList<>();
        augmentedEdges = new HashSet<>();

        // In the protocol properties, use "server1" to indicate the keys belonging to server1.
        for(int i = 1; i <= numOfServers; i++) {
            HashSet<Integer> keys = new HashSet<>();
            if (protocolProperties.get("server" + i) != null) {
                for (String key: protocolProperties.get("server" + i)) {
                    keys.add(new Integer(key));
                }
            }
            sharedGraph.add(keys);
        }

        // In the protocol properties, use "client1" to indicate the replicas this client could access.
        for (int i = 1; i <= numOfServers; i++) {
            for (String s: protocolProperties.get("client" + i)) {
                int v2 = Integer.parseInt(s);
                if (v2 != i) {
                    augmentedEdges.add(Edge.newBuilder().setVertex1(i).setVertex2(v2).build());
                    augmentedEdges.add(Edge.newBuilder().setVertex1(v2).setVertex2(i).build());
                }
            }
        }

        // Get the local timestamp.
        generateDependencies();

        // Now, we get the timestamp=(e_ij│e_ij∈E ̂}∪{e_ji│e_ji∈E ̂}∪{e_jk│∃augmented (i,e_jk )-loop in G}.
        // Then we need to compute the intersection with E.
        timestamp.entrySet().removeIf(e -> AdMatrix[e.getKey().getVertex1()-1][e.getKey().getVertex2()-1].isEmpty());
    }

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
                timestamp.put(Edge.newBuilder().setVertex1(serverId).setVertex2(i+1).build(), (long) 0);
                timestamp.put(Edge.newBuilder().setVertex1(i+1).setVertex2(serverId).build(), (long) 0);
            }
        }
        // These edges represent the replica and local replica could be accessed by the same client.
        for (Edge e: augmentedEdges) {
            if (e.getVertex1() == serverId || e.getVertex2() == serverId) {
                timestamp.put(e, (long) 0);
            }
        }

        // Secondly, the tracked edges include edge ejk in (i, ejk)-loop.
        loop();
    }

    // Detect loops containing serverId by using DFS.
    private void loop() {
        // Use DFS to identify each loop containing server_id.
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
            Edge e = Edge.newBuilder().setVertex1(Id).setVertex2(i).build();
            if(!AdMatrix[i-1][Id-1].isEmpty() || augmentedEdges.contains(e)) {
                if (i == serverId) {
                    // A loop is formed. Check whether each edge in the loop should be tracked or not.
                    if(path.size() < 3)
                        continue;
                    path.add(serverId);
                    checkEdges(path);
                    path.remove(path.size()-1);
                } else if (!visited[i-1]) {
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
                    distinct(AdMatrix[path.get(k)-1][path.get(k+1)-1], union) && (distinct(AdMatrix[path.get(k+1)-1][path.get(k+2)-1], union)
                    || augmentedEdges.contains(Edge.newBuilder().setVertex1(path.get(k+1)).setVertex2(path.get(k+2)).build()))) {
                union.addAll(sharedGraph.get(path.get(k)-1));
                int j = k + 2;
                for (; j <= path.size()-2; j++) {
                    // The edges in this loop between path[k+1]->path[k] and path[j+1]->path[j] are not supposed to be tracked.
                    if (!distinct(AdMatrix[path.get(j)-1][path.get(j+1)-1], union)&&
                            !augmentedEdges.contains(Edge.newBuilder().setVertex1(path.get(j)).setVertex2(path.get(j+1)).build())) {
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
                    timestamp.put(Metadata.Edge.newBuilder().setVertex1(path.get(k+1)).setVertex2(path.get(k)).build(), (long) 0);
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
    public void handleClientMessage(ClientMessageAgent cma) {
        if (cma.getClientMessage().hasGetMessage()) {
            handleGetMessage(cma);
        } else if(cma.getClientMessage().hasPutMessage()) {
            handlePutMessage(cma);
        } else if (cma.getClientMessage().hasTMessage()) {
            handleTimestampMessage(cma);
        } else if (cma.getClientMessage().hasUpdateTMessage()) {
            handleUpdateTimestampMessage(cma);
        }
    }

    private void handleUpdateTimestampMessage(ClientMessageAgent cma) {
        // This function is to delay until this replica's timestamp is no smaller than the client's timestamp.
        // Firstly, check whether the current timestamp is large enough or not.
        if (handleMessagesNow(cma.getClientMessage().getUpdateTMessage().getTimestampsList())) {
            // If yes, merge the received timestamp and local timestamp.
            List<Dependency> timestampList = cma.getClientMessage().getUpdateTMessage().getTimestampsList();
            try {
                writeLock.lock();
                for (Dependency dep: timestampList) {
                    if (timestamp.containsKey(dep.getEdge()) && dep.getVersion() > timestamp.get(dep.getEdge())) {
                        timestamp.put(dep.getEdge(), dep.getVersion());
                    }
                }
            } finally {
                writeLock.unlock();
            }
            ClientReply cr = ClientReply.newBuilder().setUpdateTReply(UpdateTMessageReply.newBuilder().setStatus(true).build()).build();
            cma.sendReply(cr);
        } else {
            // If not, add this timestamp request into pending client message list.
            try {
                clientWriteLock.lock();
                pendingClientMessages.add(cma);
            } finally {
                clientWriteLock.unlock();
            }
        }
    }

    private void handleTimestampMessage(ClientMessageAgent cma) {
        if (cma.getClientMessage().getTMessage().getRequestTimestamp()) {
            ClientReply cr = ClientReply.newBuilder().setTReply(TimestampReply.newBuilder().addAllTimestamps(getTimestampList()).
                    addAllKeys(sharedGraph.get(serverId-1)).build()).build();
            cma.sendReply(cr);
        }
    }

    private List<Dependency> getTimestampList() {
        List<Dependency> temp_timestamps = new ArrayList<>();
        try {
            readLock.lock();
            for (Map.Entry<Edge, Long> entry: timestamp.entrySet()) {
                Dependency dep = Dependency.newBuilder().setEdge(entry.getKey()).setVersion(entry.getValue()).build();
                temp_timestamps.add(dep);
            }
        } finally {
            readLock.unlock();
        }
        return temp_timestamps;
    }

    private void handlePutMessage(ClientMessageAgent cma) {
        PutMessage pm = cma.getClientMessage().getPutMessage();

        //Firstly, check whether the request could be handled right now or not.
        if (!handleMessagesNow(pm.getTimestampsList())) {
            // If not, add this request into the pending client message list.
            try {
                clientWriteLock.lock();
                pendingClientMessages.add(cma);
            } finally {
                clientWriteLock.unlock();
            }
        } else {
            // This request could be handled right now.
            boolean status = handlePutMessages(pm);
            ClientReply cr;
            if (status) {
                protocolLOGGER.info("PUT " + pm.getKey() + " " + pm.getValue() + "at " + System.currentTimeMillis());
                cr = ClientReply.newBuilder().setPutReply(PutReply.newBuilder().setStatus(true).build()).build();
            } else {
                cr = ClientReply.newBuilder().setPutReply(PutReply.newBuilder().setStatus(false).build()).build();
            }
            cma.sendReply(cr);
        }
    }

    private boolean handlePutMessages(PutMessage pm) {
        // Insert the key value into local database.
        Storage.StorageStatus ss = insert(pm.getKey(), pm.getValue());

        if (ss == Storage.StorageStatus.FAILURE) {
            return false;
        }

        // Secondly, update the local timestamp.
        int bucket = -1;
        try {
            bucket = findBucket(pm.getKey());
        } catch (NoSuchAlgorithmException e) {
            protocolLOGGER.severe("Problem finding bucket for key " + pm.getKey());
        }
        try {
            writeLock.lock();
            for (Dependency dep: pm.getTimestampsList()) {
                int v1 = dep.getEdge().getVertex1(), v2 = dep.getEdge().getVertex2();
                if (timestamp.containsKey(dep.getEdge())) {
                    if (v1 == serverId && AdMatrix[v1-1][v2-1].contains(bucket)) {
                        timestamp.put(dep.getEdge(), timestamp.get(dep.getEdge()) + 1);
                    } else if (timestamp.get(dep.getEdge()) < dep.getVersion()) {
                        timestamp.put(dep.getEdge(), dep.getVersion());
                    }
                }
            }
        } finally {
            writeLock.unlock();
        }

        // Thirdly, send replicate message to other replicas which also contain key.
        ServerMessage sm = ServerMessage.newBuilder().setReplicateMessage(
                ReplicateMessage.newBuilder().setKey(pm.getKey()).setRec(pm.getValue()).setServerId(serverId).
                        addAllTimestamps(getTimestampList()).build()).build();
        for (int i = 0; i < numOfServers; i++) {
            if(AdMatrix[serverId-1][i].contains(bucket)) {
                sendToServerViaChannel(Integer.toString(i + 1), sm);
            }
        }

        return true;
    }

    private Integer findBucket(String key) throws NoSuchAlgorithmException {
        long hash = edu.msu.cse.dkvf.Utils.getMd5HashLong(key);
        return (int) (hash % numOfBuckets);
    }

    private void handleGetMessage(ClientMessageAgent cma) {
        GetMessage gm = cma.getClientMessage().getGetMessage();

        //Firstly, check whether the request could be handled right now or not.
        if (!handleMessagesNow(gm.getTimestampsList())) {
            // If not, add this request into the pending client message list.
            try {
                clientWriteLock.lock();
                pendingClientMessages.add(cma);
            } finally {
                clientWriteLock.unlock();
            }
        } else {
            // This request could be handled right now.
            Record rec = handleGetMessages(gm);
            ClientReply cr;
            if (rec == null) {
                cr = ClientReply.newBuilder().setGetReply(GetReply.newBuilder().setStatus(false).build()).build();
            } else {
                cr = ClientReply.newBuilder().setGetReply(GetReply.newBuilder().setStatus(true).setRecord(rec).build()).build();
            }
            cma.sendReply(cr);
        }
    }

    private boolean handleMessagesNow(List<Dependency> timestampList) {
        // Firstly, check whether this request should be handled now or not.
        // for each e_ji, local_timestamp's version >= received timestamp's version.
        try {
            readLock.lock();
            for (Dependency dep: timestampList) {
                if (timestamp.containsKey(dep.getEdge()) && dep.getEdge().getVertex2() == serverId && dep.getVersion() > timestamp.get(dep.getEdge())) {
                    return false;
                }
            }
        } finally {
            readLock.unlock();
        }
        return true;
    }

    private Record handleGetMessages(GetMessage gm) {
        List<Record> result = new ArrayList<>();
        Storage.StorageStatus ss = read(gm.getKey(), (Record rec) -> true, result);
        if (ss == Storage.StorageStatus.SUCCESS) {
            return result.get(0);
        } else {
            return null;
        }
    }

    @Override
    public void handleServerMessage(ServerMessage sm) {
        if (sm.hasReplicateMessage()) {
            handleReplicateMesssage(sm);
        }
    }

    private void handleReplicateMesssage(ServerMessage sm) {
        ReplicateMessage rm = sm.getReplicateMessage();

        // Firstly, add replicate message to pendings.
        pendingReplicateMessages.add(rm);
        Iterator<ReplicateMessage> iterator = pendingReplicateMessages.iterator();
        while (iterator.hasNext()){
            rm = iterator.next();
            boolean updateNow = false, flag = true;

            // This is used to store the edges ∈ E_serverId ∩ E_rm.getServerId().
            List<Pair<Edge, Long>> intersectEdges = new ArrayList<>();

            try {
                readLock.lock();
                for (Dependency dep: rm.getTimestampsList()) {
                    if (timestamp.containsKey(dep.getEdge())) {
                        intersectEdges.add(new Pair<>(dep.getEdge(), dep.getVersion()));
                    }
                    if (dep.getEdge().getVertex1() == rm.getServerId() && dep.getEdge().getVertex2() == serverId) {
                        if (timestamp.get(dep.getEdge()) < dep.getVersion() - 1) {
                            // This replicate message needs to wait for this server's update later.
                            break;
                        } else if (timestamp.get(dep.getEdge()) >= dep.getVersion()) {
                            // This server is newer than this replicate message.
                            iterator.remove();
                            break;
                        } else {
                            updateNow = true;
                        }
                    } else if (dep.getEdge().getVertex2() == serverId && timestamp.get(dep.getEdge()) < dep.getVersion()) {
                        flag = false;
                        break;
                    }
                }
            } finally {
                readLock.unlock();
            }

            if (updateNow && flag) {
                // Write the value of this replicate message to local storage.
                Storage.StorageStatus ss = insert(rm.getKey(), rm.getRec());
                if (ss == Storage.StorageStatus.SUCCESS) {
                    protocolLOGGER.info("SERVER_MESSAGE " + rm.getKey() + " " + rm.getRec() + "at " + System.currentTimeMillis());
                    // update the timestamp of current server.
                    try {
                        writeLock.lock();
                        for (Pair<Edge, Long> e: intersectEdges) {
                            timestamp.put(e.getKey(), Math.max(timestamp.get(e.getKey()), e.getValue()));
                        }
                    } finally {
                        writeLock.unlock();
                    }
                    // Remove this completed replicate message.
                    iterator.remove();
                }
            }
            intersectEdges.clear();
        }
        handlePendingClientMessages();
    }

    private void handlePendingClientMessages() {
        Iterator<ClientMessageAgent> iterator = pendingClientMessages.iterator();
        while (iterator.hasNext()) {
            ClientMessageAgent cma;
            try {
                clientReadLock.lock();
                cma = iterator.next();
            } finally {
                clientReadLock.unlock();
            }
            if (cma.getClientMessage().hasGetMessage()) {
                if (handleMessagesNow(cma.getClientMessage().getGetMessage().getTimestampsList())) {
                    Record rec = handleGetMessages(cma.getClientMessage().getGetMessage());
                    ClientReply cr;
                    if (rec == null) {
                        cr = ClientReply.newBuilder().setGetReply(GetReply.newBuilder().setStatus(false).build()).build();
                    } else {
                        cr = ClientReply.newBuilder().setGetReply(GetReply.newBuilder().setStatus(true).setRecord(rec).build()).build();
                    }
                    cma.sendReply(cr);
                    try {
                        clientWriteLock.lock();
                        iterator.remove();
                    } finally {
                        clientWriteLock.unlock();
                    }
                }
            } else if (cma.getClientMessage().hasPutMessage()) {
                if (handleMessagesNow(cma.getClientMessage().getPutMessage().getTimestampsList())) {
                    PutMessage pm = cma.getClientMessage().getPutMessage();
                    boolean status = handlePutMessages(pm);
                    ClientReply cr;
                    if (status) {
                        protocolLOGGER.info("PUT " + pm.getKey() + " " + pm.getValue() + "at " + System.currentTimeMillis());
                        cr = ClientReply.newBuilder().setPutReply(PutReply.newBuilder().setStatus(true).build()).build();
                    } else {
                        cr = ClientReply.newBuilder().setPutReply(PutReply.newBuilder().setStatus(false).build()).build();
                    }
                    cma.sendReply(cr);
                    try {
                        clientWriteLock.lock();
                        iterator.remove();
                    } finally {
                        clientWriteLock.unlock();
                    }
                }
            } else if (cma.getClientMessage().hasUpdateTMessage()) {
                if (handleMessagesNow(cma.getClientMessage().getUpdateTMessage().getTimestampsList())) {
                    List<Dependency> timestampList = cma.getClientMessage().getUpdateTMessage().getTimestampsList();
                    try {
                        writeLock.lock();
                        for (Dependency dep: timestampList) {
                            if (timestamp.containsKey(dep.getEdge()) && dep.getVersion() > timestamp.get(dep.getEdge())) {
                                timestamp.put(dep.getEdge(), dep.getVersion());
                            }
                        }
                    } finally {
                        writeLock.unlock();
                    }
                    ClientReply cr = ClientReply.newBuilder().setUpdateTReply(UpdateTMessageReply.newBuilder().setStatus(true).build()).build();
                    cma.sendReply(cr);
                    // remove this timestamp request from pending client message list.
                    try {
                        clientWriteLock.lock();
                        iterator.remove();
                    } finally {
                        clientWriteLock.unlock();
                    }
                }
            }
        }
    }
}

