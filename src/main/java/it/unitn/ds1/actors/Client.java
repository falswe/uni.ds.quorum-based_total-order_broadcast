package it.unitn.ds1.actors;

import java.io.Serializable;
import java.util.List;
import java.util.ArrayList;

import akka.actor.*;
import it.unitn.ds1.Main.StartMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.util.Random;

public class Client extends AbstractActor {
  private static final Logger logger = LoggerFactory.getLogger(Client.class);
  protected final int id;
  protected List<ActorRef> replicas;

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

  private int randomReplica() {
    Random rand = new Random();
    return rand.nextInt(replicas.size());
  }

  private int randomValue() {
    Random rand = new Random();
    return rand.nextInt();
  }

  public static class ReadRequest implements Serializable {
  }

  public static class ReadResponse implements Serializable {
    public final int value;

    public ReadResponse(int value) {
      this.value = value;
    }
  }

  public static class WriteRequest implements Serializable {
    public int new_value;

    public WriteRequest(int new_value) {
      this.new_value = new_value;
    }
  }

  public void onReadResponse(ReadResponse msg) {
    logger.info("Client {} read {}", id, msg.value);
  }

  public void onStartMessage(StartMessage msg) throws InterruptedException {
    setReplicas(msg);
    int rand_replica_id = randomReplica();
    replicas.get(rand_replica_id).tell(new ReadRequest(), getSelf());
    logger.info("Client {} sent read request to replica {}", id, rand_replica_id);

    rand_replica_id = randomReplica();
    int rand_new_value = randomValue();
    replicas.get(rand_replica_id).tell(new WriteRequest(rand_new_value), getSelf());
    logger.info("Client {} sent write request to replica {}", id, rand_replica_id);

    // wait a little bit
    try {
      Thread.sleep(10000); // 1000 millisecondi = 1 secondo
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    rand_replica_id = randomReplica();
    replicas.get(rand_replica_id).tell(new ReadRequest(), getSelf());
    logger.info("Client {} sent read request to replica {}", id, rand_replica_id);
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
        .match(ReadResponse.class, this::onReadResponse)
        .match(StartMessage.class, this::onStartMessage)
        .build();
  }
}
