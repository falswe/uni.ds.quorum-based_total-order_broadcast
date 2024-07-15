package it.unitn.ds1.actors;

import java.io.Serializable;

import akka.actor.*;

public class Replica extends AbstractActor{
  protected final int id;
  
  int value;

  public Replica(int id) {
    super();
    this.id = id;
  }

  public static class getValueRequest implements Serializable {}

  public static class setValueRequest implements Serializable {}

  private void onGetValueRequest(getValueRequest msg) {
  }

  private void onSetValueRequest(setValueRequest msg) {
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
                .match(getValueRequest.class, this::onGetValueRequest)
                .match(setValueRequest.class, this::onSetValueRequest)
                .build();

  }
}
