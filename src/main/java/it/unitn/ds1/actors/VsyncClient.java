package it.unitn.ds1.actors;

import akka.actor.*;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.unitn.ds1.actors.VsyncReplica.JoinGroupMsg;
import it.unitn.ds1.actors.VsyncReplica.RdRspMsg;

public class VsyncClient extends AbstractActor {

  // needed for our logging framework
  private static final Logger logger = LoggerFactory.getLogger(Replica.class);

  // replicas (initial group, current and proposed views)
  private final List<ActorRef> replicas;

  /*-- Actor constructors --------------------------------------------------- */
  public VsyncClient() {
    replicas = new ArrayList<>();
  }

  static public Props props() {
    return Props.create(VsyncClient.class, VsyncClient::new);
  }

  /*-- Message classes ------------------------------------------------------ */

  public static class RdRqMsg implements Serializable {
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
    logger.info("Client {} received read response with value {} of replica {}", getSelf(), m.v, getSender());
  }

  private void onJoinGroupMsg(JoinGroupMsg msg) {

    // initialize group
    replicas.addAll(msg.group);
    
    // schedule Read Request
    getContext().system().scheduler().scheduleOnce(
        Duration.create(2, TimeUnit.SECONDS), // when to send the message
        replicas.get(0), // destination actor reference
        new RdRqMsg(), // the message to send
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
