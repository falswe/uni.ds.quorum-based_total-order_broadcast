package it.unitn.ds1.actors;

import java.io.Serializable;

import akka.actor.*;
import it.unitn.ds1.actors.Client.ReadRequest;
import it.unitn.ds1.actors.Client.ReadResponse;
import it.unitn.ds1.actors.Client.WriteRequest;
import it.unitn.ds1.actors.Client.WriteResponse;

public class Replica extends AbstractActor{
  protected final int id;
  
  int value;

  public Replica(int id) {
    super();
    this.id = id;
  }

  private void onReadRequest(ReadRequest msg) {
    getSender().tell(new ReadResponse(value), getSelf());
  }

  private void onWriteRequest(WriteRequest msg) {
    //send it to the coordinator
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
