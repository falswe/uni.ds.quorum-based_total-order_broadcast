package it.unitn.ds1.actors;

import akka.actor.Props;
import it.unitn.ds1.actors.Client.WriteRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Coordinator extends Replica {
  private static final Logger logger = LoggerFactory.getLogger(Coordinator.class);

  public Coordinator() {
    super(-1); // the coordinator has the id -1
  }

  static public Props props() {
    return Props.create(Coordinator.class, () -> new Coordinator());
  }

  private void onWriteRequest(WriteRequest msg) {
    logger.info("Received write request");
    // send UPDATE to all the replicas and wait for Q(N/2)+1 ACK messages
  }
}
