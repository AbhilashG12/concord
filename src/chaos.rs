#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet, VecDeque};
    use crate::raft::{RaftNode, Role, NodeId, RequestVoteArgs, AppendEntriesArgs, RequestVoteReply, AppendEntriesReply};
    use crate::storage::mock::MockStorage;

    #[derive(Clone, Debug)]
    enum Message {
        RequestVote(RequestVoteArgs),
        RequestVoteResponse(RequestVoteReply),
        AppendEntries(AppendEntriesArgs),
        AppendEntriesResponse(AppendEntriesReply),
    }

    #[derive(Clone, Debug)]
    struct Envelope {
        from: NodeId,
        to: NodeId,
        msg: Message,
    }

    struct ChaosSimulator {
        nodes: HashMap<NodeId, RaftNode>,
        network_queue: VecDeque<Envelope>,
        // If (A, B) is in the set, A cannot send messages to B (Partition)
        partitions: HashSet<(NodeId, NodeId)>,
        crashed: HashSet<NodeId>,
    }

    impl ChaosSimulator {
        fn new(node_ids: Vec<NodeId>) -> Self {
            let mut nodes = HashMap::new();
            for &id in &node_ids {
                let storage = Box::new(MockStorage { term: 0, voted_for: None, log: vec![] });
                nodes.insert(id, RaftNode::recover(id, storage));
            }
            Self {
                nodes,
                network_queue: VecDeque::new(),
                partitions: HashSet::new(),
                crashed: HashSet::new(),
            }
        }

        // --- CHAOS CONTROLS ---

        fn crash(&mut self, id: NodeId) {
            self.crashed.insert(id);
        }

        fn reboot(&mut self, id: NodeId) {
            self.crashed.remove(&id);
            // In a real simulator, we'd extract the MockStorage and pass it to recover().
            // For simplicity here, we assume reboot clears volatile state but keeps disk.
            if let Some(node) = self.nodes.get_mut(&id) {
                node.role = Role::Follower;
                node.leader_id = None;
                node.votes_received = 0;
            }
        }

        fn partition(&mut self, group_a: &[NodeId], group_b: &[NodeId]) {
            for &a in group_a {
                for &b in group_b {
                    self.partitions.insert((a, b));
                    self.partitions.insert((b, a));
                }
            }
        }

        fn heal_network(&mut self) {
            self.partitions.clear();
        }

        // --- DISCRETE EVENT ENGINE ---

        /// Triggers a candidate election deterministically
        fn trigger_election(&mut self, id: NodeId, peers: &[NodeId]) {
            if self.crashed.contains(&id) { return; }
            
            let node = self.nodes.get_mut(&id).unwrap();
            node.become_candidate();
            
            let args = RequestVoteArgs {
                term: node.current_term,
                candidate_id: node.id,
                last_log_index: node.log.len() as u64,
                last_log_term: node.log.last().map(|e| e.term).unwrap_or(0),
            };

            for &peer in peers {
                if peer != id {
                    self.network_queue.push_back(Envelope {
                        from: id, to: peer, msg: Message::RequestVote(args.clone())
                    });
                }
            }
        }

        /// Processes one message from the network queue
        fn tick(&mut self, cluster_peers: &[NodeId]) -> bool {
            if let Some(env) = self.network_queue.pop_front() {
                // CHAOS INJECTION: Drop packet if partitioned or target crashed
                if self.crashed.contains(&env.to) || self.crashed.contains(&env.from) || 
                   self.partitions.contains(&(env.from, env.to)) {
                    return true;
                }

                // Process message and optionally queue a reply
                let reply = {
                    let target_node = self.nodes.get_mut(&env.to).unwrap();
                    match env.msg {
                        Message::RequestVote(args) => {
                            Some(Message::RequestVoteResponse(target_node.handle_request_vote(args)))
                        }
                        Message::AppendEntries(args) => {
                            Some(Message::AppendEntriesResponse(target_node.handle_append_entries(args)))
                        }
                        Message::RequestVoteResponse(reply) => {
                            if reply.term > target_node.current_term {
                                target_node.become_follower(reply.term, None);
                            } else if target_node.role == Role::Candidate && reply.vote_granted {
                                target_node.votes_received += 1;
                                if target_node.votes_received >= (cluster_peers.len() / 2) + 1 {
                                    target_node.become_leader(cluster_peers);
                                }
                            }
                            None
                        }
                        Message::AppendEntriesResponse(reply) => {
                            if reply.term > target_node.current_term {
                                target_node.become_follower(reply.term, None);
                            }
                            None // In a full implementation, update match_index here
                        }
                    }
                };

                if let Some(msg) = reply {
                    self.network_queue.push_back(Envelope {
                        from: env.to, to: env.from, msg
                    });
                }
                
                // Continuous invariant checking
                self.verify_invariants();
                return true;
            }
            false
        }

        fn run_until_idle(&mut self, peers: &[NodeId]) {
            while self.tick(peers) {}
        }

        // --- INVARIANT VERIFICATION ---

        fn verify_invariants(&self) {
            self.verify_election_safety();
            self.verify_log_matching();
        }

        fn verify_election_safety(&self) {
            let mut leaders_per_term: HashMap<u64, Vec<NodeId>> = HashMap::new();
            for node in self.nodes.values() {
                if node.role == Role::Leader {
                    leaders_per_term.entry(node.current_term).or_default().push(node.id);
                }
            }
            for (term, leaders) in leaders_per_term {
                assert!(leaders.len() <= 1, 
                    "FATAL: Split brain! Term {} has multiple leaders: {:?}", term, leaders);
            }
        }

        fn verify_log_matching(&self) {
            let nodes: Vec<&RaftNode> = self.nodes.values().collect();
            for i in 0..nodes.len() {
                for j in (i + 1)..nodes.len() {
                    let n1 = nodes[i];
                    let n2 = nodes[j];
                    let min_len = std::cmp::min(n1.log.len(), n2.log.len());
                    
                    for idx in 0..min_len {
                        // Log Matching Property: If index and term match, the entire preceding log MUST match.
                        if n1.log[idx].term == n2.log[idx].term {
                            assert_eq!(n1.log[0..=idx], n2.log[0..=idx],
                                "FATAL: Log divergence detected between Node {} and Node {} at index {}", 
                                n1.id, n2.id, idx);
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn test_network_partition_split_brain_recovery() {
        let peers = vec![1, 2, 3, 4, 5];
        let mut sim = ChaosSimulator::new(peers.clone());

        // 1. Elect Node 1
        sim.trigger_election(1, &peers);
        sim.run_until_idle(&peers);
        assert_eq!(sim.nodes.get(&1).unwrap().role, Role::Leader);

        // 2. DISASTER: Network partition isolates Node 1 and 2 from 3, 4, and 5
        sim.partition(&[1, 2], &[3, 4, 5]);

        // 3. Node 3 triggers an election in the larger partition. It should win (3 out of 5 votes).
        sim.trigger_election(3, &peers);
        sim.run_until_idle(&peers);
        
        // At this exact moment, Node 1 still thinks it is leader of Term 1. 
        // Node 3 is the actual leader of Term 2. 
        assert_eq!(sim.nodes.get(&1).unwrap().role, Role::Leader);
        assert_eq!(sim.nodes.get(&3).unwrap().role, Role::Leader);
        
        // This is safe because Node 1 is mathematically incapable of achieving quorum.
        sim.verify_invariants(); // Our engine verifies Election Safety automatically here!

        // 4. HEAL: The partition drops.
        sim.heal_network();
        
        // 5. Node 3 sends a heartbeat to the entire cluster.
        sim.network_queue.push_back(Envelope {
            from: 3, to: 1, 
            msg: Message::AppendEntries(AppendEntriesArgs {
                term: sim.nodes.get(&3).unwrap().current_term,
                leader_id: 3,
                prev_log_index: 0, prev_log_term: 0, entries: vec![], leader_commit: 0,
            })
        });
        sim.run_until_idle(&peers);

        // 6. Node 1 receives the heartbeat, realizes its term (1) is older than Node 3's term (2), 
        // and safely steps down.
        assert_eq!(sim.nodes.get(&1).unwrap().role, Role::Follower);
        assert_eq!(sim.nodes.get(&3).unwrap().role, Role::Leader);
    }
}