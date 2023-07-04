# Midterm Report

1. Changes to initial assumptions
   - Our initial report is still mostly accurate, however we have had to make some changes to our initial approach:
     - Originally we intended to implement a Chord algorithm resistant to byzantine attacks. (See the S-Chord_full.pdf paper)
     - We ran into problems with this, as it requires multiple other security critical algorithms (like secure multiparty computation), for which crates (libraries) are not yet available. We considered implementing these ourselves, but this would go beyond the scope of this project.
     - Byzantine defense is *very* difficult and resource intensive. Applying the methods described in the paper would require a very large amount of additional inter-node communication.
     - Even if we were to implement this, cloud services make it very easy for an attacker to spin up a large amount of nodes for a short timeframe that eclipses the total amount of nodes in the network.
     - -> There is no silver bullet for byzantine defense, and we will not be able to implement a solution as good as the one of our reference paper in the given timeframe.

2. Architecture
     - Our architecture separates the DHT from the API communication.
     - For the API side, the DHT is a black-box datastructure that performs like a normal hash table.
     - We make heavy use of multithreading/processing, using tokio (green) threads for all asynchronous workloads, like I/O.
     - Since we can't implement the full byzantine-attack-resistant protocol, we will secure our Chord implementation by assigning node IDs based on their IP.
     - We are currently considering runtime checks for node misbehaviour to evict nodes from the network.

3. Peer-to-peer protocol
     - We are using Rust channels for inter-node communication (this allows us to serialize/deserialize entire structs typesafe and integrity checked)
     - All inter-node messages will be specified in peer_messages.rs (this is still work in progress)
     - Rust forces us to handle all possible errors at all stages of our program. If we encounter an error in the communication with a node at any stage, we will gracefully terminate our connection with that node.

     1. API Messages
   
        As specified in the assignment for DHT, we should accept and process the following requests:
   
        - DHT PUT
        - DHT GET
        - DHT SUCCESS
        - DHT FAILURE
        
        We introduced an additional API Message, `DHT SHUTDOWN` which allows us to shut down a node gracefully through the API.
        The package has a fixed size and does not contain any other information:
        ```
        +--------+--------+--------+--------+
        |        4        |  DHT SHUTDOWN   |
        +--------+--------+--------+--------+
        ```

    2. P2P Messages
   
       We employ the following messages in our peer to peer communication

       - JoinRequest
       - JoinSuccess
       - JoinFailure
       - 

4. Future Work

   - Proper Node Joining: Currently the join implementation does not fully work yet
   - Stabilization and Finger Table update: Due to the non-working join method, we are unable to test the stabilize system

5. Workload Distribution

    It is difficult to describe the work distribution on a macro scale, as we often work together simultaneously (Pair Programming).
   Valentin primarily focuses on writing the actual code, leveraging his extensive experience as a Rust programmer. On the other hand, Eddie's role involves transforming the Pseudocode into functions that align with our architecture.

6. Effort spent for the project
   - We spend a lot of time on the initial design, as it is crucial to ensure we have a good architectural design, as this will otherwise cost us lots of time in the long run
   - Another substantial effort was spent on trying to build a good architecture for SChord, especially for the exhaustive communication infrastructure between the nodes.
   - 

