There is no unsolved bug in out raft implementation.
Our test coverages of "node_candidate_state.go", "node_leader_state.go", and "node_follower_state.go" are all above 90%, as required.

Test cases we covered:

1. Failure modes
   1.1 TestLeaderFailsAndRejoins: Test scenario that the leader is partitioned then rejoins back

   1.2 TestFollowerPartitionedAndRejoinWithNewLog: Test scenario that a follower is partitioned. While it is partitioned, an new log entry is added to leader and replicates. Then the follower rejoins.

   1.3 TestCandidateAndLeaderFallbackAfterPartition: Test scenario that all followers are partitioned and set one of the partitioned follower's term to a large number and rejoin. Cover the cases of candidate and leader fallback.

   1.4 TestPartition: Test scenario that the cluster is partitioned into 2 clusters: one with leader and first follower; other with remaining 3 followers. Then join back.

2. Election:
   2.1 TestVote_Follower: Test making sure follower the would behave correctly when handling RequestVote.

   2.2 TestVote_Candidate: Test making sure candidate would behave correctly when handling RequestVote.

   2.3 TestVote_Leader: Test making sure candidate would behave correctly when handling RequestVote.

   2.4 TestInit: Tests that nodes can successfully join a cluster and elect a leader.

   2.5 TestNewElection: Tests that if a leader is partitioned from its followers, a new leader is elected.

3. Client Interaction:
   3.1 TestClientInteraction_Leader: Test making sure leaders can register the client and process the request from clients properly.

   3.2 TestClientInteraction_Follower: Test making sure the follower would reject the registration and requests from clients with correct messages.

   3.3 TestClientInteraction_Candidate: Test making sure the candidate would reject the registration and requests from clients with correct messages.

4. AppendEntries:
   4.1 TestAppendEntriesFromClient: Test making sure leaders can register the client and process the request from clients properly. Making sure that heartbeats replicate logs correctly under multiple partition scenarios

   4.2 TestAppendEntriesTerm: Test the AppendEntriesRequest with the term lower than the follower's term

   4.3 TestAppendEntriesLogIndex: Test AppendEntriesRequest with the prevLogIndex higher the follower's lastLogIndex

   4.4 TestAppendEntriesLogTerm: Test AppendEntriesRequest with PrevLogIndex and PrevLogTerm not matching
