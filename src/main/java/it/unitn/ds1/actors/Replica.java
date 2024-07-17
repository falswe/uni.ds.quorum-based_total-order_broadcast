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

public class Replica extends AbstractActor {
  private static final Logger logger = LoggerFactory.getLogger(Replica.class);
  protected final int id;
  protected ActorRef coordinator;
  private int value = 5;
  private int new_value;

  private final static int UPDATE_TIMEOUT = 1000; // timeout for the update from coordinator, ms
  private final static int WRITEOK_TIMEOUT = 1000; // timeout for the writeok from coordinator, ms
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

  // schedule a Timeout message in specified time

  void setUpdateTimeout(int time) {
    getContext().system().scheduler().scheduleOnce(
        Duration.create(time, TimeUnit.MILLISECONDS),
        getSelf(),
        new UpdateTimeout(), // the message to send
        getContext().system().dispatcher(), getSelf());
  }

  void setWriteOkTimeout(int time) {
    getContext().system().scheduler().scheduleOnce(
        Duration.create(time, TimeUnit.MILLISECONDS),
        getSelf(),
        new UpdateTimeout(), // the message to send
        getContext().system().dispatcher(), getSelf());
  }

  public static class Ack implements Serializable {
  }

  public static class UpdateTimeout implements Serializable {
  }

  public static class WriteOkTimeout implements Serializable {
  }

  public static class ReplicaAlive implements Serializable {
  }

  private void onReadRequest(ReadRequest msg) {
    logger.info("Replica {} received read request", id);
    getSender().tell(new ReadResponse(value), getSelf());
  }

  private void onWriteRequest(WriteRequest msg) {
    logger.info("Replica {} received write request", id);
    coordinator.tell(new WriteRequest(msg.new_value), getSender());
    setUpdateTimeout(UPDATE_TIMEOUT);
  }

  private void onUpdateRequest(UpdateRequest msg) {
    update_received = true;
    logger.info("Replica {} received update request", id);
    new_value = msg.new_value;
    coordinator.tell(new Ack(), getSelf());
    setWriteOkTimeout(WRITEOK_TIMEOUT);
  }

  private void onWriteOk(WriteOk msg) {
    writeok_received = true;
    this.value = this.new_value;
  }

  public void onUpdateTimeout(UpdateTimeout msg) {
    if (!update_received) {
      // TODO: assume coordinator crashed
    }
  }

  public void onWriteOkTimeout(WriteOkTimeout msg) {
    if (!writeok_received) {
      // TODO: assume coordinator crashed
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
