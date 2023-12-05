package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Set;

public class DependencyWorker extends AbstractBehavior<DependencyWorker.Message> {

    ////////////////////
    // Actor Messages //
    ////////////////////

    public interface Message extends AkkaSerializable {
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
        ActorRef<LargeMessageProxy.Message> dependencyMinerLargeMessageProxy;
        int taskId;
        // for the first column
        String key1;
        String key2;
        // for the second column
        String key3;
        String key4;

        boolean isStringColumn;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ColumnReceiver implements Message, LargeMessageProxy.LargeMessage {
        private static final long serialVersionUID = -4667745204456518160L;
        int taskId;
        Column column;

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

        final ActorRef<Receptionist.Listing> listingResponseAdapter = context.messageAdapter(Receptionist.Listing.class, ReceptionistListingMessage::new);
        context.getSystem().receptionist().tell(Receptionist.subscribe(DependencyMiner.dependencyMinerService, listingResponseAdapter));

        this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);
    }

    /////////////////
    // Actor State //
    /////////////////

    private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;
    private final HashMap<CompositeKey, Column> columnOfStrings = new HashMap<>();
    private final HashMap<CompositeKey, Column> columnOfNumbers = new HashMap<>();
    ////////////////////
    // Actor Behavior //
    ////////////////////

    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(ReceptionistListingMessage.class, this::handle)
                .onMessage(TaskMessage.class, this::handle)
                .onMessage(ColumnReceiver.class, this::handle)
                .build();
    }

    private Behavior<Message> handle(ReceptionistListingMessage message) {
        Set<ActorRef<DependencyMiner.Message>> dependencyMiners = message.getListing().getServiceInstances(DependencyMiner.dependencyMinerService);
        for (ActorRef<DependencyMiner.Message> dependencyMiner : dependencyMiners)
            dependencyMiner.tell(new DependencyMiner.RegistrationMessage(this.getContext().getSelf(), this.largeMessageProxy));
        return this;
    }

    private Behavior<Message> handle(TaskMessage message) {
        this.getContext().getLog().info("New Task {}", message.getTaskId());
        if (message.isStringColumn()) {
            if (!columnOfStrings.containsKey(new CompositeKey(message.getKey3(), message.getKey4()))) {
                this.getContext().getLog().info("I am worker {} and I need column the keys are {} and {}", this.getContext().getSelf().path().name(), message.getKey3(), message.getKey4());
                LargeMessageProxy.LargeMessage requestColumn = new DependencyMiner.getNeededColumnMessage(
                        this.getContext().getSelf(), message.getTaskId(), message.getKey3(), message.getKey4(), true);
                this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(requestColumn, message.getDependencyMinerLargeMessageProxy()));
            }
            if (!columnOfStrings.containsKey(new CompositeKey(message.getKey1(), message.getKey2()))) {
                this.getContext().getLog().info("I am worker {} and I need column the keys are {} and {}", this.getContext().getSelf().path().name(), message.getKey1(), message.getKey2());
                LargeMessageProxy.LargeMessage requestColumn = new DependencyMiner.getNeededColumnMessage(
                        this.getContext().getSelf(), message.getTaskId(), message.getKey1(), message.getKey2(), true);
                this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(requestColumn, message.getDependencyMinerLargeMessageProxy()));
            }


        } else {

        }

        return this;
    }

    private Behavior<Message> handle(ColumnReceiver message) {

        if (message.column.getType().equals("string")) {
            this.columnOfStrings.put(new CompositeKey(message.column.getNameOfDataset(), message.column.getNameOfColumn()), message.column);
        } else {
            this.columnOfNumbers.put(new CompositeKey(message.column.getNameOfDataset(), message.column.getNameOfColumn()), message.column);
        }

        return this;
    }
}
