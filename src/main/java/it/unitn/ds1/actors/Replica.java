package it.unitn.ds1.actors;

import java.io.Serializable;

import akka.actor.*;
import it.unitn.ds1.actors.Client.ReadRequest;
import it.unitn.ds1.actors.Client.ReadResponse;
import it.unitn.ds1.actors.Client.WriteRequest;
import it.unitn.ds1.actors.Client.WriteResponse;
import it.unitn.ds1.snapshotexercise.Bank;

public class Replica extends AbstractActor{
  protected final int id;
  
  int value = 5;

  public Replica(int id) {
    super();
    this.id = id;
  }

  // a simple logging function
  void print(String s) {
    System.out.format("%2d: %s\n", id, s);
  }

  static public Props props(int id) {
    return Props.create(Replica.class, () -> new Replica(id));
  }

  private void onReadRequest(ReadRequest msg) {
    print("received read request");
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
