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

def match(getOps, allPutOps):
    for op in getOps:
        tempOp = allPutOps[int(op.clientId)-1][int(op.sourceOpIdx)-1]
        if op.key != tempOp.key or op.value != tempOp.value:
            return False
    return True

def extractOps(path, count, idx):
    # A list used to store operations extracted from the corresponding log file.
    putOps = []
    getOps = []
    putMapping = []
    getMapping = []

    with open(path) as f:
        while count > 0:
            line = f.readline()
            if "PUT" in line:
                line = line.strip()
                temp = line.split(' ', 4)
                putOps.append(put(temp[2], temp[4]))
                count = count - 1
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
                count = count - 1
                getMapping.append(idx)
                idx = idx + 1

    return (putOps, getOps, putMapping, getMapping)

def checkServerTime(path, count):
    idx = 0
    time = 0

    with open(path) as f:
        while count > 0:
            line = f.readline()
            if "PUT" in line:
                line = f.readline()
                line = f.readline().strip()
                tempIdx = int(line.split(' ', 1)[1])
                if tempIdx - idx != 1:
                    return (False, 0)
                idx = tempIdx
                line = f.readline().strip()
                tempTime = int(line.split(' ', 1)[1])
                if tempTime < time:
                    return (False, 0)
                time = tempTime
                count = count - 1

    return (True, time)

def checkClientTime(path, idx, endTime):
    operationCount = 0

    with open(path) as f:
        line = f.readline()
        while line:
            if "PUT" in line:
                line = f.readline()
                line = f.readline().strip()
                tempIdx = int(line.split(' ', 1)[1])
                if tempIdx - idx != 1:
                    return (False, 0)
                idx = tempIdx
                line = f.readline().strip()
                temp = line.split(' ', 3)
                if int(temp[1]) > int(temp[3]) or int(temp[1]) < endTime:
                    return (False, 0)
                endTime = int(temp[3])
                operationCount = operationCount + 1
            if "GET" in line:
                line = f.readline()
                line = f.readline()
                line = f.readline().strip()
                temp = line.split(' ', 3)
                if int(temp[1]) > int(temp[3]) or int(temp[1]) < endTime:
                    return (False, 0)
                endTime = int(temp[3])
                operationCount = operationCount + 1
            line = f.readline()

    return (True, operationCount)

def getRecordCount(clientPath, serverPath):
    with open(clientPath) as f:
        line = f.readline()
        while line:
            if "PUT" in line:
                line = f.readline()
                line = f.readline().strip()
                return int(line.split(' ', 1)[1]) - 1
            line = f.readline()
    count = 0
    with open(serverPath) as f:
        line = f.readline()
        while line:
            if "PUT" in line:
                count = count + 1
            line = f.readline()
    return count
    
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

    # extract all put operations for all clients.
    for i in range(n):
        serverPath = args.path + "/server" + str(i+1) + "/logs/protocol_log"
        clientPath = args.path + "/client" + str(i+1) + "/logs/protocol_log"

        recordCount = getRecordCount(clientPath, serverPath)

        # Check the timestamp of all put and get operations.
        (flag, time) = checkServerTime(serverPath, recordCount)
        if not flag:
            print("Server Fail.")
            sys.exit()
        (flag, operationCount) = checkClientTime(clientPath, recordCount, time)
        if not flag:
            print("Client Fail.")
            sys.exit()

        # match the returned value of get operations with value of corresponding put operations. 
        # Firstly, extract the put operations (present records) from server log.
        # At the same time, extract the mapping from allPutOps/allGetOps to allOps.
        putOps, getOps, putMapping, getMapping = extractOps(serverPath, recordCount, 0)
        allPutOps.append(putOps)
        mapPutOps.append(putMapping)
        # Secondly, extract the put and get operations from client log.
        # At the same time, extract the mapping from allPutOps/allGetOps to allOps.
        putOps, getOps, putMapping, getMapping = extractOps(clientPath, operationCount, recordCount)
        allPutOps[i] = allPutOps[i] + putOps
        mapPutOps[i] = mapPutOps[i] + putMapping
        allGetOps.append(getOps)
        mapGetOps.append(getMapping)
        
    for i in range(n):
        # According to clientId and SourceOpIdx to compare the value of put and get operations.
        if not match(allGetOps[i], allPutOps):
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
