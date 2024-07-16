package it.unitn.ds1.actors;

import akka.actor.Props;
import it.unitn.ds1.actors.Client.WriteRequest;

public class Coordinator extends Replica{

  public Coordinator() {
    super(-1); // the coordinator has the id -1
  }
  
  static public Props props() {
    return Props.create(Coordinator.class, () -> new Coordinator());
  }

  private void onWriteRequest(WriteRequest msg) {
    //send UPDATE to all the replicas and wait for Q(N/2)+1 ACK messages
  }
}
