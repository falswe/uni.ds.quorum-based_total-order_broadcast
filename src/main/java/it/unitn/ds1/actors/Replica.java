package it.unitn.ds1.actors;

import java.io.Serializable;

import akka.actor.*;
import it.unitn.ds1.actors.Client.ReadRequest;
import it.unitn.ds1.actors.Client.ReadResponse;
import it.unitn.ds1.actors.Client.WriteRequest;
import it.unitn.ds1.actors.Client.WriteResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Replica extends AbstractActor {
  private static final Logger logger = LoggerFactory.getLogger(Replica.class);
  protected final int id;
  int value = 5;

  public Replica(int id) {
    super();
    this.id = id;
  }

  static public Props props(int id) {
    return Props.create(Replica.class, () -> new Replica(id));
  }

  private void onReadRequest(ReadRequest msg) {
    logger.info("Replica {} received read request", id);
    getSender().tell(new ReadResponse(value), getSelf());
  }

  private void onWriteRequest(WriteRequest msg) {
    logger.info("Replica {} received write request", id);
    // send it to the coordinator
    getSender().tell(new WriteResponse(), getSelf());
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
        .match(ReadRequest.class, this::onReadRequest)
        .match(WriteRequest.class, this::onWriteRequest)
        .build();
  }
}
