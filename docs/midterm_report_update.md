# Midterm Report Update:

## 1.Field type:

We are using a data serialization library called "[channels-rs](https://crates.io/crates/channels)" to serialize and
deserialize our data.
This library allows us to transmit a struct/enum over TCP directly.
This is the protocol
schematic: ![](https://raw.githubusercontent.com/threadexio/channels-rs/master/spec/assets/packet-diagram-dark.svg)
Most importantly for us this means that we do *not* have to interact with a raw byte-level TCP socket.
This is also why we used the word "channel" in our report - this interface aligns with the inter-thread communication
interface of Rust, but instead of between threads, a struct is transmitted over any sender/receiver; in this case a TCP
socket.

Example:

```rust
tx.send(PeerMessage::GetNode(key)) ?;
```

## 2. Messages types:

Since the exam we have done some refinements on the theory of our protocol.
We will implement the Chord algorithm, and for now have planned the following messages:

Note that "key" refers to the ID of a node (generated from the hash of its IP address *and* to the storage hash used for
content, both mapping over the same key space)

1. GetNode(key) -- This is the "backbone message" of our Chord-DHT. It can be sent to *any node* in the network, which
   will respond with the IP address of the node that follows the given key in the ring.
   This can be used to find content in the network, *and* it can be used in the join process. The joining node simply
   asks his introducer to resolve all finger table entries for him.
   The resolve itself will propagate recursively through the network along the finger tables of each node, until the
   node that follows the given ID is found.
2. GetValue(key) -- This message retrieves content from *one node and one node only*.
   It *must* be sent to the node that is responsible for storing *that specific id* and will **not** propagate through
   the network.
   (So the responsible node must first be resolved using GetNode(key))
3. InsertValue(key, value) -- This message inserts content into the network and is the counterpart to GetValue(id).
   Once again it must only be sent to the node responsible for the key the value is to be stored under.
4. SplitNode(key) -- This message informs our successor that we have entered the ring,
   whereupon the successor will relinquish all key/value pairs that now fall into our keyspace.

All messages will also have an associated "response" message that is sent back to the sender as answer for a given
request.
(This is still work in progress)
We are currently also working on a design for an elegant stabilization protocol, whose message types are yet to be
determined.

## 3. Project architecture:

So far we have split the project into two "modules":
The API communication with other parts of the P2P-voip program and the DHT, which presents itself as a black-box data
structure to the API. If our DHT implementation becomes more complex, we might consider further modularization.

## 3. Threading and communication architecture:

Our entire project is async, multithreaded and multiprocess.
Every DHT-node presents a server-socket listener, from which incoming requests are dispatched to a worker thread.
The worker thread will then handle the request and send a response back to the client.
We might also employ a housekeeping thread that periodically performs node stabilization.

The network starts with a single node that manages the entire keyspace.
Joining nodes will incrementally take over parts of the keyspace from their successor.
Every node can be an introducer and will (for now) answer all GetNode(key) requests,
irregardless of whether the client making the request is actually in the network (we might couple request with
proof-of-work later).
