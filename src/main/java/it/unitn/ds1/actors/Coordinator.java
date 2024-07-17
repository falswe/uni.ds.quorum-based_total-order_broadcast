package it.unitn.ds1.actors;

import java.io.Serializable;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.ds1.Main.StartMessage;
import it.unitn.ds1.actors.Client.WriteRequest;

import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Coordinator extends Replica {
  private static final Logger logger = LoggerFactory.getLogger(Coordinator.class);
  protected List<ActorRef> replicas;
  private final Set<ActorRef> ackReceived = new HashSet<>();

  // TODO: add epoch and sequence number

  public Coordinator() {
    super(-1); // the coordinator has the id -1
  }

  static public Props props() {
    return Props.create(Coordinator.class, () -> new Coordinator());
  }

  private boolean enoughAckReceived() {
    int Q = (replicas.size() / 2) + 1;
    return ackReceived.size() >= Q;
  }

  void multicast(Serializable m) {
    for (ActorRef r : replicas)
      r.tell(m, getSelf());
  }

  // start of the different messages
  public static class UpdateRequest implements Serializable {
    public int new_value;

    public UpdateRequest(int new_value) {
      this.new_value = new_value;
    }
  }

  public static class WriteOk implements Serializable {
  }
  // end of message

  // start of the logic when receiving certain messages
  public void onStartMessage(StartMessage msg) {
    this.replicas = new ArrayList<>();
    for (ActorRef b : msg.group) {
      this.replicas.add(b);
    }
    logger.info("Coordinator starting with {} replica(s)", msg.group.size());
  }

  private void onWriteRequest(WriteRequest msg) {
    logger.info("Coordinator received write request");

    UpdateRequest update = new UpdateRequest(msg.new_value);

    // send UPDATE to all the replicas and wait for Q(N/2)+1 ACK messages
    multicast(update);
  }

  private void onAck(Ack msg) {
    // TODO: manage acks for different write request
    ackReceived.add(getSender());
    // TODO: adapt id of the replica on the logger
    logger.info("Received Ack from Replica {}", getSender());
    if (enoughAckReceived()) {
      WriteOk write = new WriteOk();
      multicast(write);
    }
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
        .match(StartMessage.class, this::onStartMessage)
        .match(WriteRequest.class, this::onWriteRequest)
        .match(Ack.class, this::onAck)
        .build();
  }
}
