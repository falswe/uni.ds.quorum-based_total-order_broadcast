package it.unitn.ds1.actors;

import akka.actor.*;
import scala.concurrent.duration.Duration;

import java.util.*;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.unitn.ds1.utils.Messages.*;

import it.unitn.ds1.utils.Functions;

public class Client extends AbstractActor {

  // needed for our logging framework
  private static final Logger logger = LoggerFactory.getLogger(Client.class);

  private final Random rnd;

  private int op_cnt;

  // replicas (initial group, current and proposed views)
  private final List<ActorRef> replicas;

  /*-- Actor constructors --------------------------------------------------- */
  public Client() {
    replicas = new ArrayList<>();
    this.rnd = new Random();
    op_cnt = 0;
  }

  static public Props props() {
    return Props.create(Client.class, Client::new);
  }

  /*-- Actor logic ---------------------------------------------------------- */

  private void onRdRspMsg(RdRspMsg m) {
    logger.info("Client {} read done {}", Functions.getId(getSelf()), m.v);
  }

  private void onJoinGroupMsg(JoinGroupMsg msg) {

    // initialize group
    replicas.addAll(msg.group);

    int rnd_op = rnd.nextInt(2);
    int rnd_time = rnd.nextInt(100);

    // if (rnd_op == 0) {
    // Read(2);
    // } else {
    Write(rnd_time);
    // }
  }

  private void Read(int milliseconds) {
    // schedule Read Request
    int rand_replica = rnd.nextInt(replicas.size());
    logger.info("Client {} read req to {}", Functions.getId(getSelf()), rand_replica);

    getContext().system().scheduler().scheduleOnce(
        Duration.create(milliseconds, TimeUnit.MILLISECONDS), // when to send the message
        replicas.get(rand_replica), // destination actor reference, a random replica
        new RdRqMsg(), // the message to send
        getContext().system().dispatcher(), // system dispatcher
        getSelf() // source of the message (myself)
    );
  }

  private void Write(int milliseconds) {
    // schedule Write Request
    int rand_replica = rnd.nextInt(replicas.size());
    int new_value = rnd.nextInt(100);
    logger.info("Client {} send write request to {} with value {}", Functions.getId(getSelf()),
        Functions.getName(replicas.get(rand_replica)),
        new_value);

    getContext().system().scheduler().scheduleOnce(
        Duration.create(milliseconds, TimeUnit.MILLISECONDS), // when to send the message
        replicas.get(rand_replica), // destination actor reference, a random replica
        new WrRqMsg(getSelf(), ++op_cnt, new_value), // the message to send
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
