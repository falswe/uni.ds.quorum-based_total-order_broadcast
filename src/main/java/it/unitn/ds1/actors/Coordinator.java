package it.unitn.ds1.actors;

import java.util.List;

import akka.actor.ActorRef;
import it.unitn.ds1.actors.Client.WriteRequest;

public class Coordinator extends Replica{

  public Coordinator(int id, List<ActorRef> replicas) {
    super(id);
  }
  
  private void onWriteRequest(WriteRequest msg) {
    //send UPDATE to all the replicas and wait for Q(N/2)+1 ACK messages
  }
}
