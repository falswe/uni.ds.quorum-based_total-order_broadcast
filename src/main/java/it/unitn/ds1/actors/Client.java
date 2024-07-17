package it.unitn.ds1.actors;

import akka.actor.*;
import it.unitn.ds1.utils.Messages.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.Duration;
import scala.util.Random;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;

/**
 * The Client class represents a client in the distributed system.
 * It sends read and write requests to replicas.
 */
public class Client extends AbstractActor {
  private static final Logger logger = LoggerFactory.getLogger(Client.class);
  protected final int id; // Client ID
  protected List<ActorRef> replicas; // List of replica actors

  public Client(int id) {
    super();
    this.id = id;
    this.replicas = new ArrayList<>();
  }

  static public Props props(int id) {
    return Props.create(Client.class, () -> new Client(id));
  }

  private void setReplicas(StartMessage sm) {
    for (ActorRef b : sm.group) {
      this.replicas.add(b);
    }
    logger.info("Client {} starting with {} replica(s)", id, sm.group.size());
  }

  public void onReadResponse(ReadResponse msg) {
    logger.info("Client {} read {}", id, msg.value);
  }

  public void onStartMessage(StartMessage msg) {
    try {
      setReplicas(msg);
      int rand_replica_id = new Random().nextInt(replicas.size());
      replicas.get(rand_replica_id).tell(new ReadRequest(), getSelf());
      logger.info("Client {} sent read request to replica {}", id, rand_replica_id);

      rand_replica_id = new Random().nextInt(replicas.size());
      int rand_new_value = new Random().nextInt();
      replicas.get(rand_replica_id).tell(new WriteRequest(rand_new_value), getSelf());
      logger.info("Client {} sent write request to replica {}", id, rand_replica_id);

      // Use scheduler for delay
      getContext().system().scheduler().scheduleOnce(
          Duration.create(10, TimeUnit.SECONDS),
          () -> {
            int new_rand_replica_id = new Random().nextInt(replicas.size());
            ;
            replicas.get(new_rand_replica_id).tell(new ReadRequest(), getSelf());
            logger.info("Client {} sent read request to replica {}", id, new_rand_replica_id);
          },
          getContext().system().dispatcher());
    } catch (Exception e) {
      logger.error("Client {} encountered an error", id, e);
    }
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
        .match(ReadResponse.class, this::onReadResponse)
        .match(StartMessage.class, this::onStartMessage)
        .build();
  }
}
