package it.unitn.ds1.actors;

import akka.actor.*;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.unitn.ds1.actors.Replica.JoinGroupMsg;
import it.unitn.ds1.actors.Replica.RdRspMsg;

import it.unitn.ds1.utils.Functions;

public class Client extends AbstractActor {

  // needed for our logging framework
  private static final Logger logger = LoggerFactory.getLogger(Client.class);
  
  private final Random rnd;

  // replicas (initial group, current and proposed views)
  private final List<ActorRef> replicas;

  /*-- Actor constructors --------------------------------------------------- */
  public Client() {
    replicas = new ArrayList<>();
    this.rnd = new Random();
  }

  static public Props props() {
    return Props.create(Client.class, Client::new);
  }

  /*-- Message classes ------------------------------------------------------ */

  public static class RdRqMsg implements Serializable {
  }

  public static class WrRqMsg implements Serializable {
    public final int new_value;

    public WrRqMsg(final int new_value){
      this.new_value = new_value;
    }
  }

  /*-- Actor logic ---------------------------------------------------------- */

  @Override
  public void preStart() {

    // schedule Read Request
    /*getContext().system().scheduler().scheduleOnce(
        Duration.create(2, TimeUnit.SECONDS), // when to send the message
        replicas.get(0), // destination actor reference
        new RdRqMsg(), // the message to send
        getContext().system().dispatcher(), // system dispatcher
        getSelf() // source of the message (myself)
    );*/

  }

  private void onRdRspMsg(RdRspMsg m) {
    logger.info("Client {} received read response with value {} of replica {}", Functions.getId(getSelf()), m.v, Functions.getId(getSender()));
  }

  private void onJoinGroupMsg(JoinGroupMsg msg) {

    // initialize group
    replicas.addAll(msg.group);
    
    // schedule Write Request
    int rand_replica = rnd.nextInt(0,replicas.size());
    int new_value = rnd.nextInt(0,100);
    logger.info("Client {} send write request to replica {} with value {}", Functions.getId(getSelf()), rand_replica, new_value);
    getContext().system().scheduler().scheduleOnce(
        Duration.create(2, TimeUnit.SECONDS), // when to send the message
        replicas.get(rand_replica), // destination actor reference, a random replica
        new WrRqMsg(new_value), // the message to send
        getContext().system().dispatcher(), // system dispatcher
        getSelf() // source of the message (myself)
    );
  }

  // Here we define the mapping between the received message types
  // and our actor methods
  @Override
  public Receive createReceive() {
    return receiveBuilder()
        .match(RdRspMsg.class, this::onRdRspMsg)
        .match(JoinGroupMsg.class, this::onJoinGroupMsg)
        .build();
  }
}
