package it.unitn.ds1.old_actors;

import akka.actor.ActorRef;
import akka.actor.Props;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.unitn.ds1.utils.Functions;
import it.unitn.ds1.utils.LegacyMessages.*;

import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;

/**
 * The Coordinator class extends the Replica class and adds coordination
 * functionality.
 * It manages the update process and ensures consistency across replicas.
 */
public class LegacyCoordinator extends LegacyReplica {
  private static final Logger logger = LoggerFactory.getLogger(LegacyCoordinator.class);
  protected List<ActorRef> replicas; // List of replica actors
  private final Set<ActorRef> ackReceived = new HashSet<>();
  private final Set<ActorRef> replicasAlive = new HashSet<>();

  // TODO: add epoch and sequence number

  private final static int BROADCAST_TIMEOUT = 1000; // Timeout for the broadcast to all replicas, ms
  private final static int CONFIRMATION_TIMEOUT = 500; // Timeout for the alive confirmation from the replica, ms

  public LegacyCoordinator() {
    super(-1); // The coordinator has the id -1
  }

  static public Props props() {
    return Props.create(LegacyCoordinator.class, () -> new LegacyCoordinator());
  }

  private boolean enoughAckReceived() {
    int Q = (replicas.size() / 2) + 1;
    return ackReceived.size() >= Q;
  }

  public void onStartMessage(StartMessage msg) {
    this.replicas = new ArrayList<>();
    for (ActorRef b : msg.group) {
      this.replicas.add(b);
    }
    logger.info("Coordinator starting with {} replica(s)", msg.group.size());
    Functions.setTimeout(getContext(), BROADCAST_TIMEOUT, getSelf(), new BroadcastTimeout());
    ;
  }

  private void onWriteRequest(WriteRequest msg) {
    try {
      logger.info("Coordinator received write request from replica {} with value {}", msg.sender_id, msg.new_value);

      UpdateRequest update = new UpdateRequest(msg.new_value);

      //Functions.multicast(update, replicas, getSelf());
    } catch (Exception e) {
      logger.error("Coordinator encountered an error during write request", e);
    }
  }

  private void onAck(Ack msg) {
    try {
      ackReceived.add(getSender());
      logger.info("Coordinator received Ack from Replica {}", msg.sender_id);
      if (enoughAckReceived()) {
        WriteOk write = new WriteOk();
        //Functions.multicast(write, replicas, getSelf());
      }
    } catch (Exception e) {
      logger.error("Coordinator encountered an error during acknowledgment handling", e);
    }
  }

  public void onBroadcastTimeout(BroadcastTimeout msg) {
    try {
      logger.debug("Coordinator sent out a broadcast message to all replicas");
      Functions.setTimeout(getContext(), BROADCAST_TIMEOUT, getSelf(), new BroadcastTimeout());
      AreYouStillAlive confAlive = new AreYouStillAlive();
      //Functions.multicast(confAlive, replicas, getSelf());
      Functions.setTimeout(getContext(), CONFIRMATION_TIMEOUT, getSelf(), new ConfirmationTimeout());
    } catch (Exception e) {
      logger.error("Coordinator encountered an error during broadcast timeout handling", e);
    }
  }

  public void onReplicaAlive(ReplicaAlive msg) {
    logger.debug("Coordinator got a hearbeat message from replica {}", msg.sender_id);
    replicasAlive.add(getSender());
  }

  public void onConfirmationTimeout(ConfirmationTimeout msg) {
    try {
      logger.debug("Coordinator reached it's confirmation timeout");
      if (replicas.size() != replicasAlive.size()) {
        logger.warn("Some replicas did not respond. Initiating failure handling.");
        // TODO: remove the replicas which are crashed
      }
    } catch (Exception e) {
      logger.error("Coordinator encountered an error during confirmation timeout handling", e);
    }
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
        .match(StartMessage.class, this::onStartMessage)
        .match(WriteRequest.class, this::onWriteRequest)
        .match(BroadcastTimeout.class, this::onBroadcastTimeout)
        .match(Ack.class, this::onAck)
        .match(BroadcastTimeout.class, this::onBroadcastTimeout)
        .match(ReplicaAlive.class, this::onReplicaAlive)
        .match(ConfirmationTimeout.class, this::onConfirmationTimeout)
        .build();
  }
}
