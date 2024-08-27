package it.unitn.ds1.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.unitn.ds1.utils.Messages;
import it.unitn.ds1.utils.Functions;

public class Client extends AbstractActor {
  private static final Logger logger = LoggerFactory.getLogger(Client.class);
  private static final int MAX_VALUE = 100;

  private int operationCount;
  private final List<ActorRef> replicas;
  private final Random random;

  public Client() {
    this.replicas = new ArrayList<>();
    this.operationCount = 0;
    this.random = new Random();
  }

  static public Props props() {
    return Props.create(Client.class, Client::new);
  }

  private void onClientRead(Messages.Client.Read msg) {
    sendReadRequest(msg.replicaId);
  }

  private void onClientWrite(Messages.Client.Write msg) {
    sendWriteRequest(msg.replicaId);
  }

  private void onReadResponse(Messages.RdRspMsg msg) {
    logger.info("Client {} read value: {}", Functions.getId(getSelf()), msg.value);
  }

  private void onJoinGroup(Messages.JoinGroupMsg msg) {
    replicas.addAll(msg.group);
    logger.info("Client {} joined group with {} replicas", Functions.getId(getSelf()), replicas.size());
  }

  private void sendReadRequest(int replicaId) {
    logger.info("Client {} sending read request to replica {}", Functions.getId(getSelf()), replicaId);
    Functions.tellDelay(new Messages.RdRqMsg(), getSelf(), replicas.get(replicaId));
  }

  private void sendWriteRequest(int replicaId) {
    int newValue = random.nextInt(MAX_VALUE);
    logger.info("Client {} sending write request to {} with value {}",
        Functions.getId(getSelf()),
        Functions.getName(replicas.get(replicaId)),
        newValue);

    Functions.tellDelay(new Messages.WrRqMsg(getSelf(), ++operationCount, newValue), getSelf(),
        replicas.get(replicaId));
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
        .match(Messages.Client.Read.class, this::onClientRead)
        .match(Messages.Client.Write.class, this::onClientWrite)
        .match(Messages.RdRspMsg.class, this::onReadResponse)
        .match(Messages.JoinGroupMsg.class, this::onJoinGroup)
        .build();
  }
}
