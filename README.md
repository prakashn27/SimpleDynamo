# SimpleDynamo
This is a simplified version of Amazon Dynamo style key value storage. 

# To Do:
1) Partitioning,
2) Replication,
3) Failure handling

# Steps followed in implementing 
## Step 1: Writing the Content Provider
Just like the previous assignment, the content provider should implement all storage functionalities. For example, it should create server and client threads (if this is what you decide to implement), open sockets, and respond to incoming requests. When writing your system, you can make the following assumptions:
Just like the previous assignment, you need to support insert/query/delete operations. Also, you need to support @ and * queries. 
There are always 5 nodes in the system. There is no need to implement adding/removing nodes from the system.
However, there can be at most 1 node failure at any given time. We will emulate a failure only by force closing an app instance. We will not emulate a failure by killing an entire emulator instance.
All failures are temporary; you can assume that a failed node will recover soon, i.e., it will not be permanently unavailable during a run.
When a node recovers, it should copy all the object writes it missed during the failure. This can be done by asking the right nodes and copy from them.
Please focus on correctness rather than performance. Once you handle failures correctly, if you still have time, you can improve your performance.

Your content provider should support concurrent read/write operations.

Your content provider should handle a failure happening at the same time with read/write operations.

Replication should be done exactly the same way as Dynamo does. In other words, a (key, value) pair should be replicated over three consecutive partitions, starting from the partition that the key belongs to.
Unlike Dynamo, there are two things you do not need to implement.

Virtual nodes: Your implementation should use physical nodes rather than virtual nodes, i.e., all partitions are static and fixed.

Hinted handoff: Your implementation do not need to implement hinted handoff. This means that when there is a failure, it is OK to replicate on only two nodes.

All replicas should store the same value for each key. This is “per-key” consistency. There is no consistency guarantee you need to provide across keys. More formally, you need to implement per-key linearizability.

Each content provider instance should have a node id derived from its emulator port. This node id should be obtained by applying the above hash function (i.e., genHash()) to the emulator port. For example, the node id of the content provider instance running on emulator-5554 should be, node_id = genHash(“5554”). This is necessary to find the correct position of each node in the Dynamo ring.

Your content provider’s URI should be “content://edu.buffalo.cse.cse486586.simpledynamo.provider”, which means that any app should be able to access your content provider using that URI. This is already defined in the template, so please don’t change this. Your content provider does not need to match/support any other URI pattern.

We have fixed the ports & sockets.
Your app should open one server socket that listens on 10000.
You need to use run_avd.py and set_redir.py to set up the testing environment.
The grading will use 5 AVDs. The redirection ports are 11108, 11112, 11116, 11120, and 11124.
You should just hard-code the above 5 ports and use them to set up connections.
Please use the code snippet provided in PA1 on how to determine your local AVD.
emulator-5554: “5554”
emulator-5556: “5556”
emulator-5558: “5558”
emulator-5560: “5560”
emulator-5562: “5562”
Any app (not just your app) should be able to access (read and write) your content provider. As with the previous assignment, please do not include any permission to access your content provider.


The following is a guideline for your content provider based on the design of Amazon Dynamo:

###Membership
Just as the original Dynamo, every node can know every other node. This means that each node knows all other nodes in the system and also knows exactly which partition belongs to which node; any node can forward a request to the correct node without using a ring-based routing.

###Request routing
Unlike Chord, each Dynamo node knows all other nodes in the system and also knows exactly which partition belongs to which node.
Under no failures, a request for a key is directly forwarded to the coordinator (i.e., the successor of the key), and the coordinator should be in charge of serving read/write operations.

###Quorum replication
For linearizability, you can implement a quorum-based replication used by Dynamo.
Note that the original design does not provide linearizability. You need to adapt the design.
The replication degree N should be 3. This means that given a key, the key’s coordinator as well as the 2 successor nodes in the Dynamo ring should store the key.
Both the reader quorum size R and the writer quorum size W should be 2.
The coordinator for a get/put request should always contact other two nodes and get a vote from each (i.e., an acknowledgement for a write, or a value for a read).
For write operations, all objects can be versioned in order to distinguish stale copies from the most recent copy.
For read operations, if the readers in the reader quorum have different versions of the same object, the coordinator should pick the most recent version and return it.

### Chain replication
Another replication strategy you can implement is chain replication, which provides linearizability.
If you are interested in more details, please take a look at the following paper: http://www.cs.cornell.edu/home/rvr/papers/osdi04.pdf
In chain replication, a write operation always comes to the first partition; then it propagates to the next two partitions in sequence. The last partition returns the result of the write.
A read operation always comes to the last partition and reads the value from the last partition.

### Failure handling
Handling failures should be done very carefully because there can be many corner cases to consider and cover.
Just as the original Dynamo, each request can be used to detect a node failure.
For this purpose, you can use a timeout for a socket read; you can pick a reasonable timeout value, e.g., 100 ms, and if a node does not respond within the timeout, you can consider it a failure.
Do not rely on socket creation or connect status to determine if a node has failed. Due to the Android emulator networking setup, it is not safe to rely on socket creation or connect status to judge node failures. Please use an explicit method to test whether an app instance is running or not, e.g., using a socket read timeout as described above.
When a coordinator for a request fails and it does not respond to the request, its successor can be contacted next for the request.


