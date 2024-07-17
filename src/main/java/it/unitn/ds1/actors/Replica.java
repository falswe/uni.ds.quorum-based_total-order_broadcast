package it.unitn.ds1.actors;

import java.io.Serializable;

import akka.actor.*;
import it.unitn.ds1.actors.Client.ReadRequest;
import it.unitn.ds1.actors.Client.ReadResponse;
import it.unitn.ds1.actors.Client.WriteRequest;
import it.unitn.ds1.actors.Coordinator.UpdateRequest;
import it.unitn.ds1.actors.Coordinator.WriteOk;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Replica extends AbstractActor {
  private static final Logger logger = LoggerFactory.getLogger(Replica.class);
  protected final int id;
  protected ActorRef coordinator;
  private int value = 5;
  private int new_value;

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

  public static class Ack implements Serializable {
  }

  private void onReadRequest(ReadRequest msg) {
    logger.info("Replica {} received read request", id);
    getSender().tell(new ReadResponse(value), getSelf());
  }

  private void onWriteRequest(WriteRequest msg) {
    logger.info("Replica {} received write request", id);
    coordinator.tell(new WriteRequest(msg.new_value), getSender());
  }

  private void onUpdateRequest(UpdateRequest msg) {
    logger.info("Replica {} received update request", id);
    new_value = msg.new_value;
    coordinator.tell(new Ack(), getSelf());
  }

  private void onWriteOk(WriteOk msg) {
    this.value = this.new_value;
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
        .match(ReadRequest.class, this::onReadRequest)
        .match(WriteRequest.class, this::onWriteRequest)
        .match(UpdateRequest.class, this::onUpdateRequest)
        .match(WriteOk.class, this::onWriteOk)
        .build();
  }
}
