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
import lombok.Setter;

import java.util.*;

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
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TaskMessage implements Message, LargeMessageProxy.LargeMessage {
        private static final long serialVersionUID = -4667745204456518160L;
        ActorRef<LargeMessageProxy.Message> dependencyMinerLargeMessageProxy;
        int taskId;
        // for the first column1
        int key1;
        // for the second column1
        int key2;

        boolean isStringColumn;

    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ColumnReceiver implements Message, LargeMessageProxy.LargeMessage {
        private static final long serialVersionUID = -4667745204456518160L;
        int taskId;
        boolean gotBothColumns;
        boolean isStringColumn;
        HashSet<String> column1;
        HashSet<String> column2;
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
    private final HashMap<Integer, HashSet<String>> columnOfStrings = new HashMap<>();
    private final HashMap<Integer, HashSet<String>> columnOfNumbers = new HashMap<>();
    private TaskMessage taskMessage;
    private int key1;
    private int key2;
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
        this.taskMessage = message;
        key1 = message.getKey1();
        key2 = message.getKey2();

        this.getContext().getLog().info("The keys are {} : {}", key1, key2);
        this.getContext().getLog().info("New Task {}", message.getTaskId());
        // This is for if the column1 is a string column1
        if (message.isStringColumn()) {
            // if I need both columns
            if (!columnOfStrings.containsKey(key1) && !columnOfStrings.containsKey(key2)) {
                this.getContext().getLog().info("I am worker {} and I need a String column1 and column2", this.getContext().getSelf().path().name());
                LargeMessageProxy.LargeMessage requestColumn = new DependencyMiner.getNeededColumnMessage(
                        this.getContext().getSelf(), message.getTaskId(), message.getKey1(), message.getKey2(), true, true);
                this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(requestColumn, message.getDependencyMinerLargeMessageProxy()));
            } else if (!columnOfStrings.containsKey(key1)) {
                this.getContext().getLog().info("I am worker {} and I need a String column1", this.getContext().getSelf().path().name());
                LargeMessageProxy.LargeMessage requestColumn = new DependencyMiner.getNeededColumnMessage(
                        this.getContext().getSelf(), message.getTaskId(), key1, -1, false, true);
                this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(requestColumn, message.getDependencyMinerLargeMessageProxy()));
            } else if (!columnOfStrings.containsKey(key2)) {
                this.getContext().getLog().info("I am worker {} and I need a String column1", this.getContext().getSelf().path().name());
                LargeMessageProxy.LargeMessage requestColumn = new DependencyMiner.getNeededColumnMessage(
                        this.getContext().getSelf(), message.getTaskId(), -1, key2, false, true);
                this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(requestColumn, message.getDependencyMinerLargeMessageProxy()));
            } else
                findingIND();
            //This is if the column1 is a number column1
        } else {
            if (!columnOfNumbers.containsKey(key1) && !columnOfNumbers.containsKey(key2)) {
                this.getContext().getLog().info("I am worker {} and I need a number column1 and column2 ", this.getContext().getSelf().path().name());
                LargeMessageProxy.LargeMessage requestColumn = new DependencyMiner.getNeededColumnMessage(
                        this.getContext().getSelf(), message.getTaskId(), key1, key2, true, false);
                this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(requestColumn, message.getDependencyMinerLargeMessageProxy()));
            } else if (!columnOfNumbers.containsKey(key1)) {
                this.getContext().getLog().info("I am worker {} and I need a number column1}", this.getContext().getSelf().path().name());
                LargeMessageProxy.LargeMessage requestColumn = new DependencyMiner.getNeededColumnMessage(
                        this.getContext().getSelf(), message.getTaskId(), key1, -1, false, false);
                this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(requestColumn, message.getDependencyMinerLargeMessageProxy()));
            } else if (!columnOfNumbers.containsKey(key2)) {
                this.getContext().getLog().info("I am worker {} and I need a number column1", this.getContext().getSelf().path().name());
                LargeMessageProxy.LargeMessage requestColumn = new DependencyMiner.getNeededColumnMessage(
                        this.getContext().getSelf(), message.getTaskId(), -1, key2, false, false);
                this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(requestColumn, message.getDependencyMinerLargeMessageProxy()));
            } else
                findingIND();
        }
        return this;
    }

    /*TODO: Here I need to also call the findingIND() method as soon as the data is ready. I need to change the handle(DataMessage message) method to ask for
             all keys and columns as I have to work with them in the ColumnReceiver method!!! TO start FindIND method from here
     */
    /*
    TODO: You also need to make sure that you deal with the situation where you have one pair of keys and not the other
     */
    private Behavior<Message> handle(ColumnReceiver message) {
        this.getContext().getLog().info("I am worker {} and I got a columns, the keys I needed", this.getContext().getSelf().path().name());
        if (message.gotBothColumns) {
            if (message.isStringColumn()) {
                this.columnOfStrings.put(key1, message.getColumn1());
                this.columnOfStrings.put(key2, message.getColumn2());
                this.getContext().getLog().info("I am worker {} and I got needed two string columns", this.getContext().getSelf().path().name());
            } else {
                this.columnOfNumbers.put(key1, message.getColumn1());
                this.columnOfNumbers.put(key2, message.getColumn2());
                this.getContext().getLog().info("I am worker {} and I got needed two number columns", this.getContext().getSelf().path().name());
            }
        } else {
            if (message.isStringColumn()) {
                if (message.column1 != null)
                    this.columnOfStrings.put(key1, message.getColumn1());
                else
                    this.columnOfStrings.put(key2, message.getColumn2());
                this.getContext().getLog().info("I am worker {} and I got needed column", this.getContext().getSelf().path().name());
            } else {
                if (message.column1 != null)
                    this.columnOfNumbers.put(key1, message.getColumn1());
                else
                    this.columnOfNumbers.put(key2, message.getColumn2());
                this.getContext().getLog().info("I am worker {} and I got needed column", this.getContext().getSelf().path().name());
            }
        }

        findingIND();

        return this;
    }

    private void findingIND() {

        boolean result;
        HashSet<String> column1;
        HashSet<String> column2;
        if (taskMessage.isStringColumn()) {
            column1 = columnOfStrings.get(key1);
            column2 = columnOfStrings.get(key2);
        } else {
            column1 = columnOfNumbers.get(taskMessage.getKey1());
            column2 = columnOfNumbers.get(taskMessage.getKey2());
        }
        this.getContext().getLog().info("Looking for IND");
        result = column1.containsAll(column2);
        this.getContext().getLog().info("{}", result);

        LargeMessageProxy.LargeMessage resultMessage = new DependencyMiner.CompletionMessage(
                this.getContext().getSelf(),
                taskMessage.getTaskId(),
                result,
                key1, key2,
                taskMessage.isStringColumn);

        this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(resultMessage, taskMessage.getDependencyMinerLargeMessageProxy()));

    }
}
