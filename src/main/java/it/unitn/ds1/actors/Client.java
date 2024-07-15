package it.unitn.ds1.actors;
import java.io.Serializable;
import java.util.List;

import akka.actor.*;

public class Client extends AbstractActor{
  protected final int id;
  //protected int value;
  protected List<ActorRef> replicas;  

  public Client(int id, List<ActorRef> replicas) {
    super();
    this.id = id;
    this.replicas = replicas;
  }

  public static class ReadRequest implements Serializable {

  }

  public static class ReadResponse implements Serializable {
    public ReadResponse(int value)
    {
    }
  }

  public static class WriteRequest implements Serializable {
    public final int new_value;
    public WriteRequest(int new_value) {
      this.new_value = new_value;
    }
  }

  public static class WriteResponse implements Serializable{
    public WriteResponse(){
      
    }
  }

  public void onReadResponse(ReadResponse msg){

  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
                .match(ReadResponse.class, this::onReadResponse)
                .build();

  }
  
}