import sys
import argparse
import numpy as np
from collections import namedtuple

put = namedtuple('put', 'key, value')
get = namedtuple('get', 'key, value, clientId, sourceOpIdx')
# Used to locate the corresponding operation.
loc = namedtuple('loc', 'clientId, destOpIdx')

def loop(allOps, n):
    for i in range(n):
        # Use DFS to look for loop in happened before graph.
        visited = set()
        if dfs(visited, i, 0):
            return True
    return False

def dfs(visited, clientId, opIdx):
    # The node has already been visited. Loop exists.
    if loc(clientId, opIdx) in visited:
        return True
    visited.add(loc(clientId, opIdx))
    if not allOps[clientId][opIdx]:
        return False
    for neighbor in allOps[clientId][opIdx]:
        if dfs(visited, int(neighbor.clientId), int(neighbor.destOpIdx)):
            return True
        visited.remove(neighbor)
    return False

def reduceGraph(n, allOps):
    newAllOps = []
    allMapping = []

    for i in range(n):
        ops = []
        mapping = []
        idx = 0
        for j in range(len(allOps[i])):
            # Remove such redundant vertices.
            if (len(allOps[i][j]) == 1) and (allOps[i][j][0].clientId == i) and (allOps[i][j][0].destOpIdx == j+1):
                mapping.append(idx)
            else:
                ops.append(allOps[i][j])
                mapping.append(idx)
                idx = idx + 1
        newAllOps.append(ops)
        allMapping.append(mapping)

    allOps.clear()

    for i in range(n):
        ops = []
        for j in range(len(newAllOps[i])):
            temp = []
            for k in range(len(newAllOps[i][j])):
                l = newAllOps[i][j][k]
                temp.append(loc(l.clientId, allMapping[l.clientId][l.destOpIdx]))
            ops.append(temp)
        allOps.append(ops)

    return allOps

def happenedBeforeGraph(n, allGetOps, mapPutOps, mapGetOps):
    # A list to store the happened before graph.
    allOps = []

    # Initialize each client's list.
    for i in range(n):
         # A temporary list for each client.
        ops = []
        # m represents all the operations in this client/server.
        m = len(mapPutOps[i]) + len(mapGetOps[i])
        for j in range(1, m):
            ops.append([loc(i, j)])
        ops.append([])
        allOps.append(ops)

    # Add the read from relations.
    for i in range(n):
        for j in range(len(allGetOps[i])):
            op = allGetOps[i][j]
            if int(op.clientId) != i + 1:
                allOps[int(op.clientId)-1][mapPutOps[int(op.clientId)-1][int(op.sourceOpIdx)-1]].append(loc(i, mapGetOps[i][j]))

    return allOps

def match(allGetOps, allPutOps, n, recordCound):
    for i in range(n):
        for j in range(len(allGetOps[i])):
            op = allGetOps[i][j]
            tempOp = allPutOps[int(op.clientId)-1][int(op.sourceOpIdx)-1]
            if op.key == tempOp.key and op.value == tempOp.value:
                continue
            if int(op.sourceOpIdx) + recordCount <= len(allPutOps[int(op.clientId)-1]):
                tempOp = allPutOps[int(op.clientId)-1][int(op.sourceOpIdx)+recordCount-1]
                if op.key == tempOp.key and op.value == tempOp.value:
                    allGetOps[i][j] = get(op.key, op.value, op.clientId, int(op.sourceOpIdx)+recordCount)
                    continue
            return False
    return True

def extractOps(clientPath, idx):
    # A list used to store operations extracted from the corresponding log file.
    putOps = []
    getOps = []
    putMapping = []
    getMapping = []

    with open(clientPath) as f:
        line = f.readline()
        while line:
            if "PUT" in line:
                line = line.strip()
                temp = line.split(' ', 4)
                putOps.append(put(temp[2], temp[4]))
                putMapping.append(idx)
                idx = idx + 1
            if "GET" in line:
                line = line.strip()
                temp = line.split(' ', 4)
                line = f.readline().strip()
                temp1 = line.split(' ', 1)
                line = f.readline().strip()
                temp2 = line.split(' ', 1)
                getOps.append(get(temp[2], temp[4], temp1[1], temp2[1]))
                getMapping.append(idx)
                idx = idx + 1
            line = f.readline()

    return (putOps, getOps, putMapping, getMapping)

def checkClientLog(path, n, recordCount, time):
    for i in range(n):
        clientPath = path + "/client" + str(i+1) + "/logs/protocol_log"
        endTime = time[i]
        idx = 0
        with open(clientPath) as f:
            line = f.readline()
            while line:
                if "PUT" in line:
                    line = f.readline()
                    line = f.readline().strip()
                    tempIdx = int(line.split(' ', 1)[1])
                    if tempIdx - idx != 1:
                        return False
                    idx = tempIdx
                    line = f.readline().strip()
                    temp = line.split(' ', 3)
                    if int(temp[1]) > int(temp[3]) or int(temp[1]) < endTime:
                        return False
                    endTime = int(temp[3])
                if "GET" in line:
                    line = f.readline()
                    line = f.readline()
                    line = f.readline().strip()
                    temp = line.split(' ', 3)
                    if int(temp[1]) > int(temp[3]) or int(temp[1]) < endTime:
                        return False
                    endTime = int(temp[3])
                line = f.readline()

    return True

def extractPrePutOps(recordCount, path, n, time):
    # Initialize the 2D arrays to store the present operations for each client.
    # This 2D array should be (n * recordCount)
    allPutOps = [[put("", "")]*recordCount for _ in range(n)]
    mapPutOps = []
    timeStamps = [[None]*recordCount for _ in range(n)]
    temp_map = []
    for i in range(recordCount):
        temp_map.append(i)
    for i in range(n):
        mapPutOps.append(temp_map)

    # Read each server's log file and store each PUT operation into the corresponding location in 2D array.
    # We could extract the timestamp for each put operation meanwhile.
    for i in range(n):
        serverPath = path + "/server" + str(i+1) + "/logs/protocol_log"
        with open(serverPath) as f:
            line = f.readline()
            while line:
                if "PUT" in line:
                    line = line.strip()
                    temp = line.split(' ', 4)
                    line = f.readline().strip()
                    clientId = int(line.split(' ', 1)[1])
                    line = f.readline().strip()
                    opIdx = int(line.split(' ', 1)[1])
                    line = f.readline().strip()
                    opTime = int(line.split(' ', 1)[1])
                    if opIdx <= recordCount and opTime <= time[i]:
                        allPutOps[clientId-1][opIdx-1] = put(temp[2], temp[4])
                        timeStamps[clientId-1][opIdx-1] = opTime
                line = f.readline()

    # Check whether the timestamps are ascending for the same client.
    for i in range(n):
        lastTimestamp = 0
        for j in range(recordCount):
            if timeStamps[i][j] < lastTimestamp:
                print("Server Fail.")
                sys.exit()
            lastTimestamp = timeStamps[i][j]
    
    # Return the timestamp of the last present put operations to compare with client's operations.
    return (allPutOps, mapPutOps) 

def getRecordCount(path, n, time1):
    clientPath = path + "/client1/logs/protocol_log"
    maxOpIdx = 0
    for i in range(n):
        serverPath = path + "/server" + str(i+1) + "/logs/protocol_log"
        with open(serverPath) as f:
            line = f.readline()
            while line:
                if "PUT" in line:
                    line = f.readline().strip()
                    clientId = int(line.split(' ', 1)[1])
                    line = f.readline().strip()
                    OpIdx = int(line.split(' ', 1)[1])
                    line = f.readline().strip()
                    OpTime = int(line.split(' ', 1)[1])
                    if clientId == 1 and OpTime <= time1:
                        if OpIdx > maxOpIdx:
                            maxOpIdx = OpIdx
                line = f.readline()
    return maxOpIdx

def getConstructionTime(path, n):
    time = []
    for i in range(n):
        clientPath = path + "/client" + str(i+1) + "/logs/protocol_log"
        with open(clientPath) as f:
            line = f.readline()
            while line:
                if "construction" in line:
                    line = line.strip()
                    time.append(int(line.split(' ', 4)[4]))
                line = f.readline()
    return time

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', dest='numOfServers', help='number of servers', required=True)
    parser.add_argument('-p', dest='path', help='directory of server/client log file', required=True)
    args = parser.parse_args()

    allPutOps = []
    allGetOps = []
    n = int(args.numOfServers)
    mapPutOps = []
    mapGetOps = []
    time = []

    # Get the construction time of each client.
    # The present operations are handled before the construction.
    time = getConstructionTime(args.path, n)

    # Get the value of recordcount from log files.
    recordCount = 0
    if n > 0:
        recordCount = getRecordCount(args.path, n, time[0])

    # Get all present put operations for each client from all server log files.
    # Generate the mapping from allPutOps to allOps.
    # For these present operations, the index in allPutOps is same with the index in allOps.
    (allPutOps, mapPutOps) = extractPrePutOps(recordCount, args.path, n, time) 

    # Check whether the timestamps of all operations in client log files are ascending or not.
    flag = checkClientLog(args.path, n, recordCount, time)    
    if not flag:
        print("Client Fail.")
        sys.exit()

    # Extract the put and get operations from client log.
    # Meanwhile, generate the mapping from allPutOps/allGetOps to allOps.
    for i in range(n):
        clientPath = args.path + "/client" + str(i+1) + "/logs/protocol_log"
        putOps, getOps, putMapping, getMapping = extractOps(clientPath, recordCount)
        allPutOps[i] = allPutOps[i] + putOps
        mapPutOps[i] = mapPutOps[i] + putMapping
        allGetOps.append(getOps)
        mapGetOps.append(getMapping)

    # According to clientId and SourceOpIdx to compare the value of put and get operations.
    # However, there might be operation index conflicts between the client's put operations and the present operations.
    # We could identify the relation by comparing key and value.
    if not match(allGetOps, allPutOps, n, recordCount):
        print("Value doesn't match.")
        sys.exit()

    # Rule thread execution and Rule read from verified.
    # Now let's construct the happened-before graph.
    # Each client maintains a list with length - recordCount + operationCount.
    # Each element in above list is a list containing all other elements it points to.
    allOps = happenedBeforeGraph(n, allGetOps, mapPutOps, mapGetOps)

    # Reduce the redundant vertices and edges in this graph.
    allOps = reduceGraph(n, allOps)

    # Check whether there is a loop in allOps.
    if loop(allOps, n):
        print("Verfication Fails.")
    else:
        print("Verification Succeeds.")

