package it.unitn.ds1.old_actors;

import akka.actor.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.unitn.ds1.utils.Functions;
import it.unitn.ds1.utils.Messages.*;

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

  private void onReadRequest(ReadRequest msg) {
    try {
      logger.info("Replica {} received read request from client {}", id, msg.sender_id);
      getSender().tell(new ReadResponse(id, value), getSelf());
    } catch (Exception e) {
      logger.error("Replica {} encountered an error during read request", id, e);
    }
  }

  private void onWriteRequest(WriteRequest msg) {
    try {
      logger.info("Replica {} received write request from client {} with value {}", id, msg.sender_id, msg.new_value);
      coordinator.tell(new WriteRequest(id, msg.new_value), getSender());
      Functions.setTimeout(getContext(), UPDATE_TIMEOUT, getSelf(), new UpdateTimeout());
    } catch (Exception e) {
      logger.error("Replica {} encountered an error during write request", id, e);
    }
  }

  private void onUpdateRequest(UpdateRequest msg) {
    try {
      update_received = true;
      logger.info("Replica {} received update request from the coordinator", id);
      new_value = msg.new_value;
      coordinator.tell(new Ack(id), getSelf());
      Functions.setTimeout(getContext(), WRITEOK_TIMEOUT, getSelf(), new WriteOkTimeout());
    } catch (Exception e) {
      logger.error("Replica {} encountered an error during update request", id, e);
    }
  }

  private void onWriteOk(WriteOk msg) {
    try {
      logger.info("Replica {} received WriteOk from coordinator, changed value from {} to {}", id, value, new_value);
      writeok_received = true;
      this.value = this.new_value;
    } catch (Exception e) {
      logger.error("Replica {} encountered an error during write acknowledgment", id, e);
    }
  }

  public void onUpdateTimeout(UpdateTimeout msg) {
    logger.debug("Replica {} reached it's update timeout", id);
    if (!update_received) {
      logger.error("Replica {} did not receive update in time. Coordinator might have crashed.", id);
      // TODO: Implement coordinator crash recovery
    }
  }

  public void onWriteOkTimeout(WriteOkTimeout msg) {
    logger.debug("Replica {} reached it's WriteOk timeout", id);
    if (!writeok_received) {
      logger.error("Replica {} did not receive write acknowledgment in time. Coordinator might have crashed.", id);
      // TODO: Implement coordinator crash recovery
    }
  }

  public void onAreYouStillAlive(AreYouStillAlive msg) {
    logger.debug("Replica {} was asked if it's still alive", id);
    getSender().tell(new ReplicaAlive(id), getSelf());
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
