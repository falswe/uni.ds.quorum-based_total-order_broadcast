package it.unitn.ds1.actors;

import akka.actor.ActorRef;
import akka.actor.Props;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.Duration;

import it.unitn.ds1.messages.Messages.*;

import java.util.List;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.HashSet;

/**
 * The Coordinator class extends the Replica class and adds coordination
 * functionality.
 * It manages the update process and ensures consistency across replicas.
 */
public class Coordinator extends Replica {
  private static final Logger logger = LoggerFactory.getLogger(Coordinator.class);
  protected List<ActorRef> replicas; // List of replica actors
  private final Set<ActorRef> ackReceived = new HashSet<>();
  private final Set<ActorRef> replicasAlive = new HashSet<>();

  // TODO: add epoch and sequence number

  private final static int BROADCAST_TIMEOUT = 1000; // Timeout for the broadcast to all replicas, ms
  private final static int CONFIRMATION_TIMEOUT = 500; // Timeout for the alive confirmation from the replica, ms

  public Coordinator() {
    super(-1); // The coordinator has the id -1
  }

  static public Props props() {
    return Props.create(Coordinator.class, () -> new Coordinator());
  }

  private boolean enoughAckReceived() {
    int Q = (replicas.size() / 2) + 1;
    return ackReceived.size() >= Q;
  }

  void multicast(Serializable m) {
    for (ActorRef r : replicas) {
      r.tell(m, getSelf());
    }
  }

  void setBroadcastTimeout(int time) {
    getContext().system().scheduler().scheduleOnce(
        Duration.create(time, TimeUnit.MILLISECONDS),
        getSelf(),
        new BroadcastTimeout(),
        getContext().system().dispatcher(), getSelf());
  }

  void setConfirmationTimeout(int time) {
    getContext().system().scheduler().scheduleOnce(
        Duration.create(time, TimeUnit.MILLISECONDS),
        getSelf(),
        new ConfirmationTimeout(),
        getContext().system().dispatcher(), getSelf());
  }

  public void onStartMessage(StartMessage msg) {
    this.replicas = new ArrayList<>();
    for (ActorRef b : msg.group) {
      this.replicas.add(b);
    }
    logger.info("Coordinator starting with {} replica(s)", msg.group.size());
    setBroadcastTimeout(BROADCAST_TIMEOUT);
  }

  private void onWriteRequest(WriteRequest msg) {
    try {
      logger.info("Coordinator received write request");

      UpdateRequest update = new UpdateRequest(msg.new_value);

      multicast(update);
    } catch (Exception e) {
      logger.error("Coordinator encountered an error during write request", e);
    }
  }

  private void onAck(Ack msg) {
    try {
      ackReceived.add(getSender());
      logger.info("Received Ack from Replica {}", getSender());
      if (enoughAckReceived()) {
        WriteOk write = new WriteOk();
        multicast(write);
      }
    } catch (Exception e) {
      logger.error("Coordinator encountered an error during acknowledgment handling", e);
    }
  }

  public void onBroadcastTimeout(BroadcastTimeout msg) {
    try {
      setBroadcastTimeout(BROADCAST_TIMEOUT);
      AreYouStillAlive confAlive = new AreYouStillAlive();
      multicast(confAlive);
      setConfirmationTimeout(CONFIRMATION_TIMEOUT);
    } catch (Exception e) {
      logger.error("Coordinator encountered an error during broadcast timeout handling", e);
    }
  }

  public void onReplicaAlive(ReplicaAlive msg) {
    replicasAlive.add(getSender());
  }

  public void onConfirmationTimeout(ConfirmationTimeout msg) {
    try {
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
