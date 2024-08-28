# Course Project: Quorum-based Total Order Broadcast
**Course:** Distributed Systems 1, A. A. 2023 - 2024 \
**Authors:** Luca Lunardelli and Wendelin Falschlunger

This project implements a protocol for the coordination of a group of replicas, sharing the same data, using a quorum-based approach. The system handles update requests from external clients and guarantees that all replicas eventually apply the updates in the same order. It is built using Akka.

## Table of Contents
- [General Description](#general-description)
- [Installation](#installation)
- [Usage](#usage)
- [Implementation Details](#implementation-details)
- [Logging](#logging)
- [Crash Detection and Handling](#crash-detection-and-handling)

## General Description

The project implements a protocol for coordinating a group of replicas sharing the same data, tolerating multiple node failures through a quorum-based approach. The system manages update requests from external clients, ensuring that all replicas eventually apply updates in the same order.

### Key Concepts
- **Replicas:** Akka actors identified by unique IDs, holding a single variable `value`.
- **Coordinator:** A special replica managing updates using a two-phase broadcast procedure.
- **Client Requests:** Clients can issue read and write requests to any replica.
- **Sequential Consistency:** Ensured if the client always interacts with the same replica during operations.

### Client Requests
- **Read Requests:** The replica immediately replies with its local value.
- **Write Requests:** Forwarded to the coordinator, which initiates a two-phase broadcast to update all replicas.

### Update Protocol
1. Coordinator broadcasts an `UPDATE` message with the new value to all replicas.
2. The coordinator waits for `ACK` messages from a quorum (`Q = ⌊N/2 + 1⌋) of replicas.
3. Upon receiving enough `ACKs`, the coordinator broadcasts a `WRITEOK` message.
4. Replicas apply the update and maintain a history of updates.

### Crash Detection
- Implemented using timeouts and periodic heartbeat messages from the coordinator.
- Replicas detect a crashed coordinator if they timeout while waiting for the `WRITEOK` message.

### Coordinator Election
- Uses a ring-based protocol based on replica IDs.
- The new coordinator is the replica that knows the most recent update.

### Properties
- Ensures that if a replica applies an update, all correct replicas will apply the update.
- New coordinator completes any interrupted broadcasts to maintain consistency.

## Installation

To set up the project, clone the repository and install the required dependencies:

```sh
git clone <repository_url>
cd <repository_directory>
./gradlew build
```

## Usage

Run the system using the following command:

```sh
./gradlew run
```

Ensure to configure the necessary Parameters in the configuration file before running.

## Implementation Details

- The system uses Akka for actor-based implementation.
- Unicast message exchanges between actors are reliable.
- Small random intervals are inserted between unicast transmissions to emulate network delays.
- Proper actor encapsulation is ensured, and shared objects are immutable.

## Logging

The system generates log files recording key steps of the protocol. Key log messages include:
- `Replica <ReplicaID> update <epoch number>:<sequence number> <value>`
- `Client <ClientID> read req to <ReplicaID>`
- `Client <ClientID> read done <value>`

## Crash Detection and Handling

To emulate crashes, a participant can enter the "crashed mode" where it ignores all incoming messages and stops sending anything. The system assumes crash detection does not give false positives, and reasonable values are used for timeouts.

### Crash Scenarios
- **Coordinator Crashes:** The remaining replicas elect a new coordinator using a ring-based protocol.
- **Replica Crashes:** Replicas can crash while sending stabilization of flush messages and are later recovered through manager coordination.
