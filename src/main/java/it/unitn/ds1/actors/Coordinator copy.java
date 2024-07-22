// package it.unitn.ds1.actors;

// import akka.actor.*;
// import it.unitn.ds1.actors.Client.RdRqMsg;
// import it.unitn.ds1.actors.Client.WrRqMsg;
// import it.unitn.ds1.utils.Functions;

// import java.io.Serializable;
// import java.util.*;

// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;

// public class Coordinator extends Replica {
//   // participants (initial group, current and proposed views)
//   private final Set<ActorRef> view;
//   private int viewId;

//   /*-- Actor logic ---------------------------------------------------------- */
//   private void onCrashReportMsg(CrashReportMsg msg) {

//     // remove the crashed node from view;
//     // if the view changed, update view ID and notify nodes
//     boolean viewChange = false;
//     for (ActorRef crashed : msg.crashedMembers) {
//       if (view.remove(crashed)) {
//         viewChange = true;
//       }
//     }
//     if (viewChange) {
//       viewId++;
//       ViewChangeMsg m = new ViewChangeMsg(viewId, view);
//       System.out.println(
//           getSelf().path().name() + " view " + m.viewId
//               + " of " + m.proposedView.size() + " nodes "
//               + " (" + msg.crashedMembers + " crashed - reported by "
//               + getSender().path().name() + ") " + m.proposedView);
//       Functions.multicast(m, replicas, getSelf());
//     }
//   }

//   private void onJoinNodeMsg(JoinNodeMsg msg) {

//     // add node to view;
//     // if the view changed, update view ID and notify nodes
//     if (view.add(getSender())) {
//       viewId++;
//       ViewChangeMsg m = new ViewChangeMsg(viewId, view);
//       System.out.println(
//           getSelf().path().name() + " view " + m.viewId
//               + " of " + m.proposedView.size() + " nodes "
//               + " (" + getSender() + " joining) " + m.proposedView);
//       Functions.multicast(m, replicas, getSelf());
//     }
//   }

//   private void onCrashMsg(CrashMsg msg) {
//     if (msg.nextCrash == CrashType.NotResponding) {
//       crash();
//     } else {
//       nextCrash = msg.nextCrash;
//     }
//   }

//   // Here we define the mapping between the received message types
//   // and our actor methods
//   @Override
//   public Receive createReceive() {
//     return receiveBuilder()
//         .match(JoinGroupMsg.class, this::onJoinGroupMsg)
//         .match(RdRqMsg.class, this::onRdRqMsg)
//         .match(WrRqMsg.class, this::onWrRqMsg)
//         .match(UpdRqMsg.class, this::onUpdRqMsg)
//         .match(UpdTimeout.class, this::onUpdTimeout)
//         .match(AckTimeout.class, this::onAckTimeout)
//         .match(HeartbeatMsg.class, this::onHeartbeatMsg)
//         .match(CoordinatorHeartbeatPeriod.class, this::onCoordinatorHeartbeatPeriod)
//         .match(CoordinatorHeartbeatMsg.class, this::onCoordinatorHeartbeatMsg)
//         .match(WrOk.class, this::onWrOk)
//         .match(AckMsg.class, this::onAck)
//         .match(HeartbeatPeriod.class, this::onHeartbeatPeriod)
//         .match(HeartbeatTimeout.class, this::onHeartbeatTimeout)
//         .match(HeartbeatMsg.class, this::onHeartbeatMsg)
//         .match(JoinNodeMsg.class, this::onJoinNodeMsg)
//         .match(CrashMsg.class, this::onCrashMsg)
//         .match(CrashReportMsg.class, this::onCrashReportMsg)
//         .build();
//   }
// }
