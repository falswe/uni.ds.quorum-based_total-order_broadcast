package it.unitn.ds1.actors;
import java.io.Serializable;
import java.util.List;

import akka.actor.*;

public class Node extends AbstractActor{
  protected final int id;
  protected List<ActorRef> replicas;  

  public Node(int id, List<ActorRef> replicas) {
    super();
    this.id = id;
    this.replicas = replicas;
  }

  public static class getReplicaValue implements Serializable {}

  public static class setReplicaValue implements Serializable {}

  private void onGetReplicaValue(getReplicaValue msg) {
  }

  private void onSetReplicaValue(setReplicaValue msg) {
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
                .match(getReplicaValue.class, this::onGetReplicaValue)
                .match(setReplicaValue.class, this::onSetReplicaValue)
                .build();

  }
  
}