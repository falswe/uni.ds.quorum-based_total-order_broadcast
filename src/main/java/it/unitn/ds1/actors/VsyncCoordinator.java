package it.unitn.ds1.actors;
import akka.actor.*;
import it.unitn.ds1.actors.VsyncReplica.JoinGroupMsg;
import it.unitn.ds1.actors.VsyncReplica.ViewChangeMsg;

import java.io.Serializable;
import java.util.*;

public class VsyncCoordinator extends AbstractActor {

  // participants (initial group, current and proposed views)
  private final List<ActorRef> replicas;
  private final Set<ActorRef> view;
  private int viewId;

  /*-- Actor constructors --------------------------------------------------- */
  public VsyncCoordinator() {
    replicas = new ArrayList<>();
    view = new HashSet<>(replicas);
    viewId = 0;
  }

  static public Props props() {
    return Props.create(VsyncCoordinator.class, VsyncCoordinator::new);
  }

  /*-- Message classes ------------------------------------------------------ */

  public static class CrashReportMsg implements Serializable {
    public final Set<ActorRef> crashedMembers;
    public CrashReportMsg(Set<ActorRef> crashedMembers) {
      this.crashedMembers = Collections.unmodifiableSet(crashedMembers);
    }
  }

  public static class JoinNodeMsg implements Serializable {}

  /*-- Actor logic ---------------------------------------------------------- */

  @Override
  public void preStart() {
  }

  private void multicast(Serializable m) {
    for (ActorRef r: view) {
      r.tell(m, getSelf());
    }
  }

  private void onJoinGroupMsg(JoinGroupMsg msg) {

    // initialize group
    for (ActorRef r: msg.group) {
      if (!r.equals(getSelf())) {
        this.replicas.add(r);
      }
    }

    // at the beginning, the view includes all nodes in the group
    view.addAll(replicas);
    //System.out.println(getSelf().path().name() + " initial view " + view);
  }

  private void onCrashReportMsg(CrashReportMsg msg) {

    // remove the crashed node from view;
    // if the view changed, update view ID and notify nodes
    boolean viewChange = false;
    for(ActorRef crashed : msg.crashedMembers) {
      if(view.remove(crashed)) {
        viewChange = true;
      }
    }
    if(viewChange) {
      viewId++;
      ViewChangeMsg m = new ViewChangeMsg(viewId, view);
      System.out.println(
              getSelf().path().name() + " view " + m.viewId
              + " of " + m.proposedView.size() + " nodes "
              + " (" + msg.crashedMembers + " crashed - reported by "
              + getSender().path().name() + ") " + m.proposedView
      );
      multicast(m);
    }
  }

  private void onJoinNodeMsg(JoinNodeMsg msg) {

    // add node to view;
    // if the view changed, update view ID and notify nodes
    if(view.add(getSender())) {
      viewId++;
      ViewChangeMsg m = new ViewChangeMsg(viewId, view);
      System.out.println(
              getSelf().path().name() + " view " + m.viewId
                      + " of " + m.proposedView.size() + " nodes "
                      + " (" + getSender() + " joining) " + m.proposedView
      );
      multicast(m);
    }
  }

  // Here we define the mapping between the received message types
  // and our actor methods
  @Override
  public Receive createReceive() {
    return receiveBuilder()
            .match(JoinGroupMsg.class,  this::onJoinGroupMsg)
            .match(JoinNodeMsg.class,  this::onJoinNodeMsg)
            .match(CrashReportMsg.class,  this::onCrashReportMsg)
            .build();
  }
}
