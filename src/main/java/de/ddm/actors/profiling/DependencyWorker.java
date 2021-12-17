package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import de.ddm.actors.ColumnId;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.*;

public class DependencyWorker extends AbstractBehavior<DependencyWorker.Message> {

    ////////////////////
    // Actor Messages //
    ////////////////////

    public interface Message extends AkkaSerializable, LargeMessageProxy.LargeMessage {
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ReceptionistListingMessage implements Message {
        private static final long serialVersionUID = -5246338806092216222L;
        Receptionist.Listing listing;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TaskMessage implements Message {
        private static final long serialVersionUID = -4667745204456518160L;
        ActorRef<DependencyMiner.Message> dependencyMiner;
        ColumnId referencedColumnId;
        ColumnId dependentColumnId;
        int dependentColumnFrom;
        int dependentColumnTo;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DataMessage implements Message {
        private static final long serialVersionUID = 2135984614102497577L;
        String[] referencedColumn;
        String[] maybeDependentColumnPart;
        ActorRef<LargeMessageProxy.Message> dependencyMinerLargeMessageProxy;
    }

    @NoArgsConstructor
    public static class ShutdownMessage implements Message {
        private static final long serialVersionUID = -8612352862100883350L;
    }

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "dependencyWorker";

    public static Behavior<Message> create() {
        return Behaviors.setup(DependencyWorker::new);
    }

    private DependencyWorker(ActorContext<Message> context) {
        super(context);

        final ActorRef<Receptionist.Listing> listingResponseAdapter =
                context.messageAdapter(Receptionist.Listing.class, ReceptionistListingMessage::new);
        context.getSystem().receptionist().tell(Receptionist.subscribe(DependencyMiner.dependencyMinerService,
                listingResponseAdapter));

        this.largeMessageProxy =
                this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()),
                        LargeMessageProxy.DEFAULT_NAME);
    }

    /////////////////
    // Actor State //
    /////////////////

    private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;

    private String[] referencedColumn = null;
    private String[] dependentColumnPart = null;
    private ColumnId referencedColumnId = null;
    private ColumnId dependentColumnId = null;


    ////////////////////
    // Actor Behavior //
    ////////////////////

    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(ReceptionistListingMessage.class, this::handle)
                .onMessage(TaskMessage.class, this::handle)
                .onMessage(DataMessage.class, this::handle)
                .build();
    }

    private Behavior<Message> handle(ReceptionistListingMessage message) {
        Set<ActorRef<DependencyMiner.Message>> dependencyMiners =
                message.getListing().getServiceInstances(DependencyMiner.dependencyMinerService);
        for (ActorRef<DependencyMiner.Message> dependencyMiner : dependencyMiners)
            dependencyMiner.tell(new DependencyMiner.RegistrationMessage(this.getContext().getSelf()));
        return this;
    }

    private Behavior<Message> handle(TaskMessage message) {
        this.getContext().getLog().info("Asking for work!");
        // I should probably know how to solve this task, but for now I just pretend some work...

        boolean needReferencedColumnData = !message.getReferencedColumnId().equals(this.referencedColumnId);
        this.referencedColumnId = message.getReferencedColumnId();
        this.dependentColumnId = message.getDependentColumnId();

        // Request data from dependency miner
        getContext().getLog().info("TaskMessage: referencedColumnId: " + this.referencedColumnId + ", dependentColumnId:" + this.dependentColumnId.toString());
        message.dependencyMiner.tell(new DependencyMiner.RequestDataMessage(this.largeMessageProxy,
                needReferencedColumnData ? this.referencedColumnId : null, this.dependentColumnId,
                message.dependentColumnFrom, message.dependentColumnTo));

        return this;
    }

    private Behavior<Message> handle(DataMessage message) {
        this.getContext().getLog().info("Working!");

        if (message.getReferencedColumn() != null) {
            this.referencedColumn = message.getReferencedColumn();
        }
        this.dependentColumnPart = message.maybeDependentColumnPart;

        boolean isDependencyCandidate = true;
        for (String s : this.dependentColumnPart) {
            if (Arrays.binarySearch(this.referencedColumn, s) < 0) {
                isDependencyCandidate = false;
                break;
            }
        }

        LargeMessageProxy.LargeMessage completionMessage =
                new DependencyMiner.CompletionMessage(this.getContext().getSelf(), this.referencedColumnId,
                        this.dependentColumnId, isDependencyCandidate);

        this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(completionMessage,
                message.getDependencyMinerLargeMessageProxy()));

        return this;
    }

}
