# Raft-6.824
This is an implementation of Raft for the MIT 6.824 Distributed Systems course. 

Raft is a consensus algorithm for maintaining a replicated log, similar in goal but not design to Multi-Paxos. It can be used to create a replicated state machine, for example, which can manage information about state and leadership for a large distributed system such as GFS, HDFS, or RAMCloud. The extended paper can be read here: https://raft.github.io/raft.pdf

This implementation currently contains the mechanism for leadership election, log replication, and an optimization to reduce the number of rejected RPC calls in the event of an incorrect follower. 
