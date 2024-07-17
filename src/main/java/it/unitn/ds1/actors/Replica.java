package it.unitn.ds1.actors;

import java.io.Serializable;

import akka.actor.*;
import it.unitn.ds1.actors.Client.ReadRequest;
import it.unitn.ds1.actors.Client.ReadResponse;
import it.unitn.ds1.actors.Client.WriteRequest;
import it.unitn.ds1.actors.Coordinator.AreYouStillAlive;
import it.unitn.ds1.actors.Coordinator.UpdateRequest;
import it.unitn.ds1.actors.Coordinator.WriteOk;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Replica class represents a replica in the distributed system.
 * It handles read and write requests and coordinates with the Coordinator.
 */
public class Replica extends AbstractActor {
  private static final Logger logger = LoggerFactory.getLogger(Replica.class);
  protected final int id; // Replica ID
  protected ActorRef coordinator; // Reference to the coordinator actor
  private int value = 5; // Initial value of the replica
  private int new_value;

  private final static int UPDATE_TIMEOUT = 1000; // Timeout for the update from coordinator, ms
  private final static int WRITEOK_TIMEOUT = 1000; // Timeout for the writeok from coordinator, ms
  private boolean update_received = false;
  private boolean writeok_received = false;

  public Replica(int id, ActorRef coordinator) {
    super();
    this.id = id;
    this.coordinator = coordinator;
  }

  public Replica(int id) {
    super();
    this.id = id;
  }

  static public Props props(int id, ActorRef coordinator) {
    return Props.create(Replica.class, () -> new Replica(id, coordinator));
  }

  // Schedule a Timeout message in specified time
  void setUpdateTimeout(int time) {
    getContext().system().scheduler().scheduleOnce(
        Duration.create(time, TimeUnit.MILLISECONDS),
        getSelf(),
        new UpdateTimeout(), // The message to send
        getContext().system().dispatcher(), getSelf());
  }

  void setWriteOkTimeout(int time) {
    getContext().system().scheduler().scheduleOnce(
        Duration.create(time, TimeUnit.MILLISECONDS),
        getSelf(),
        new UpdateTimeout(), // The message to send
        getContext().system().dispatcher(), getSelf());
  }

  // Messages used by the replica.
  public static class Ack implements Serializable {
  }

  public static class UpdateTimeout implements Serializable {
  }

  public static class WriteOkTimeout implements Serializable {
  }

  public static class ReplicaAlive implements Serializable {
  }

  private void onReadRequest(ReadRequest msg) {
    try {
      logger.info("Replica {} received read request", id);
      getSender().tell(new ReadResponse(value), getSelf());
    } catch (Exception e) {
      logger.error("Replica {} encountered an error during read request", id, e);
    }
  }

  private void onWriteRequest(WriteRequest msg) {
    try {
      logger.info("Replica {} received write request", id);
      coordinator.tell(new WriteRequest(msg.new_value), getSender());
      setUpdateTimeout(UPDATE_TIMEOUT);
    } catch (Exception e) {
      logger.error("Replica {} encountered an error during write request", id, e);
    }
  }

  private void onUpdateRequest(UpdateRequest msg) {
    try {
      update_received = true;
      logger.info("Replica {} received update request", id);
      new_value = msg.new_value;
      coordinator.tell(new Ack(), getSelf());
      setWriteOkTimeout(WRITEOK_TIMEOUT);
    } catch (Exception e) {
      logger.error("Replica {} encountered an error during update request", id, e);
    }
  }

  private void onWriteOk(WriteOk msg) {
    try {
      writeok_received = true;
      this.value = this.new_value;
    } catch (Exception e) {
      logger.error("Replica {} encountered an error during write acknowledgment", id, e);
    }
  }

  public void onUpdateTimeout(UpdateTimeout msg) {
    if (!update_received) {
      logger.error("Replica {} did not receive update in time. Coordinator might have crashed.", id);
      // TODO: Implement coordinator crash recovery
    }
  }

  public void onWriteOkTimeout(WriteOkTimeout msg) {
    if (!writeok_received) {
      logger.error("Replica {} did not receive write acknowledgment in time. Coordinator might have crashed.", id);
      // TODO: Implement coordinator crash recovery
    }
  }

  public void onAreYouStillAlive(AreYouStillAlive msg) {
    getSender().tell(new ReplicaAlive(), getSelf());
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
        .match(ReadRequest.class, this::onReadRequest)
        .match(WriteRequest.class, this::onWriteRequest)
        .match(UpdateRequest.class, this::onUpdateRequest)
        .match(WriteOk.class, this::onWriteOk)
        .match(UpdateTimeout.class, this::onUpdateTimeout)
        .match(WriteOkTimeout.class, this::onWriteOkTimeout)
        .match(AreYouStillAlive.class, this::onAreYouStillAlive)
        .build();
  }
}
