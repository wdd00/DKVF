# DKVF

Welcome! 

Please refer to Wiki. 
https://github.com/roohitavaf/DKVF/wiki

## New protocols

We add three new protocols based on the protocols schemed by Xiang, Vaidya and me.

* The ReplicaCentric_client and ReplicaCentric_server directories implement the protocol proposed by Xiang and Vaidya [1]. This protocol is based on the peer-to-peer architecture and the client can only access the local replica.

* The ReplicaCentric_client_client-server and ReplicaCentric_server_client-server directories implement the protocol proposed by Xiang and Vaidya [1] as well. This protocol is based on the client-server architecture and the client can access an arbitrary set of all replicas (servers).

* The ReplicaCentric_client-peer-to-peer and ReplicaCentric_server-peer-to-peer directories implement the protocol proposed by me with advisoring from Vaidya. This protocol is also based on the peer-to-peer architecture, and the client can access an arbitrary set of all replicas. 

[1]Xiang, Z., & Vaidya, N. H. (2019, July). Partially replicated causally consistent shared memory: Lower bounds and an algorithm. In Proceedings of the 2019 ACM Symposium on Principles of Distributed Computing (pp. 425-434).


