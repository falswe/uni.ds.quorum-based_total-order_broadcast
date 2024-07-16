package it.unitn.ds1.actors;
import java.io.Serializable;
import java.util.List;
import java.util.ArrayList;

import akka.actor.*;
import it.unitn.ds1.Main.StartMessage;
import it.unitn.ds1.twophasecommit.TwoPhaseCommit.VoteRequest;
import scala.util.Random;

public class Client extends AbstractActor{
  protected final int id;
  protected List<ActorRef> replicas;  

  public Client(int id) {
    super();
    this.id = id;
    this.replicas = new ArrayList<>();
  }

  // a simple logging function
  void print(String s) {
    System.out.format("%2d: %s\n", id, s);
  }

  static public Props props(int id) {
    return Props.create(Client.class, () -> new Client(id));
  }

  private void setReplicas(StartMessage sm) {
    for (ActorRef b: sm.group) {
      this.replicas.add(b);
    }
    //print("starting with " + sm.group.size() + " peer(s)");
  }

  private int randomReplica(){
    Random rand = new Random();
		int rand_id = rand.nextInt(replicas.size());
		return rand_id;
  }

  public static class ReadRequest implements Serializable {}

  public static class ReadResponse implements Serializable {
    public final int value;
    public ReadResponse(int value)
    {
      this.value = value;
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
    print("read " + msg.value);
  }

  public void onStartMessage(StartMessage msg) {                   /* Start */
    setReplicas(msg);

    int rand_replica_id = randomReplica();
    replicas.get(rand_replica_id).tell(new ReadRequest(), getSelf());
    print("Client "+id+" read req to "+rand_replica_id);
    //print("Sending vote request");
    //multicast(new VoteRequest());
    //multicastAndCrash(new VoteRequest(), 3000);
    //setTimeout(VOTE_TIMEOUT);
    //crash(5000);
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
                .match(ReadResponse.class, this::onReadResponse)
                .match(StartMessage.class, this::onStartMessage)
                .build();

  }
  
}