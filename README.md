# Raft-6.824
This is an implementation of Raft for the MIT 6.824 Distributed Systems course. 

Raft is a consensus algorithm for maintaining a replicated log, similar in goal but not design to Multi-Paxos. It can be used to create a replicated state machine, for example, which can manage information about state and leadership for a large distributed system such as GFS, HDFS, or RAMCloud. The extended paper can be read here: https://raft.github.io/raft.pdf

This implementation currently contains the mechanism for leadership election, log replication, and an optimization to reduce the number of rejected RPC calls in the event of an incorrect follower. It works by using 5 long-running goroutines: ticker, applyCh, heartbeat, commit, and maintainLog. Ticker and applyCh are run by all servers. The ticker controls the election timeout if there is no viable leader, and the applyCh is triggered after a new entry has been committed. The heartbeat loop is run by the leader only, to prevent unnecessary elections if no new entries arrive. The commit loop periodically checks if the servers have reached a consensus on any new entries. Finally, the maintainLog loop will try to send append entry RPCs to all followers, and will keep retrying until either the logs match or the leader becomes aware of a new leader elected. 

<img width="618" alt="Screen Shot 2022-11-28 at 3 52 34 PM" src="https://user-images.githubusercontent.com/39568393/204378575-ae7b698d-9e8c-4df8-8c64-68b5f5886288.png">

