package it.unitn.ds1.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.unitn.ds1.utils.Message;
import it.unitn.ds1.utils.Helper;

public class Client extends AbstractActor {

  private static final Logger logger = LoggerFactory.getLogger(Client.class);
  private static final int MAX_RANDOM_VALUE = 100;

  private int operationCount;
  private final List<ActorRef> replicas;

  public Client() {
    this.replicas = new ArrayList<>();
    this.operationCount = 0;
  }

  public static Props props() {
    return Props.create(Client.class, Client::new);
  }

  private void onRead(Message.Client.Read msg) {
    sendReadRequest(msg.replicaId);
  }

  private void onWrite(Message.Client.Write msg) {
    sendWriteRequest(msg.replicaId);
  }

  private void onReadResponse(Message.Replica.ReadResponse msg) {
    logger.info("Client {} read done {}", Helper.getId(getSelf()), msg.value);
  }

  private void onJoinGroup(Message.System.JoinGroup msg) {
    replicas.addAll(msg.group);
    logger.debug("Client {} joined group with {} replicas", Helper.getId(getSelf()), replicas.size());
  }

  private void sendReadRequest(int replicaId) {
    logger.info("Client {} sending read request to replica {}", Helper.getId(getSelf()), replicaId);
    Helper.tellDelay(new Message.Client.ReadRequest(), getSelf(), replicas.get(replicaId));
  }

  private void sendWriteRequest(int replicaId) {
    int newValue = ThreadLocalRandom.current().nextInt(MAX_RANDOM_VALUE);
    logger.info("Client {} sending write request to replica {} with value {}",
        Helper.getId(getSelf()),
        Helper.getName(replicas.get(replicaId)),
        newValue);

    Message.Client.WriteRequest writeRequest = new Message.Client.WriteRequest(getSelf(), ++operationCount, newValue);
    Helper.tellDelay(writeRequest, getSelf(), replicas.get(replicaId));
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
        .match(Message.Client.Read.class, this::onRead)
        .match(Message.Client.Write.class, this::onWrite)
        .match(Message.Replica.ReadResponse.class, this::onReadResponse)
        .match(Message.System.JoinGroup.class, this::onJoinGroup)
        .build();
  }
}
