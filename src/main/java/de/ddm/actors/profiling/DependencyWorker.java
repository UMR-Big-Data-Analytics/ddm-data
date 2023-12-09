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

import java.util.AbstractMap;
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
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TaskMessage implements Message, LargeMessageProxy.LargeMessage {
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
        boolean gotBothColumns;
        Column column;
        String key1;
        String key2;
        Column column2;
        String key3;
        String key4;

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
    private TaskMessage taskMessage;
    private String key1;
    private String key2;
    private String key3;
    private String key4;
    HashMap<AbstractMap.SimpleEntry<String, String>, CompositeKey> compositeKeyPool = new HashMap<>();
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

    private CompositeKey getCompositeKey(String subKey1, String subKey2) {
        AbstractMap.SimpleEntry<String, String> keyPair = new AbstractMap.SimpleEntry<>(subKey1, subKey2);
        CompositeKey compositeKey = compositeKeyPool.get(keyPair);
        if (compositeKey == null) {
            compositeKey = new CompositeKey(subKey1, subKey2);
            compositeKeyPool.put(keyPair, compositeKey);
        }
        return compositeKey;
    }

    private Behavior<Message> handle(TaskMessage message) {
        this.taskMessage = message;
        key1 = message.getKey1();
        key2 = message.getKey2();
        key3 = message.getKey3();
        key4 = message.getKey4();
        this.getContext().getLog().info("The keys are {} : {} and {} : {}", key1, key2, key3, key4);
        this.getContext().getLog().info("New Task {}", message.getTaskId());
        // This is for if the column is a string column
        if (message.isStringColumn()) {
            // if I need both columns
            if (!columnOfStrings.containsKey(getCompositeKey(key1, key2)) && !columnOfStrings.containsKey(getCompositeKey(key3, key4))) {
                this.getContext().getLog().info("I am worker {} and I need a column1 and column2, the keys are {} : {} and {} : {}", this.getContext().getSelf().path().name(), key1, key2, key3, key4);
                LargeMessageProxy.LargeMessage requestColumn = new DependencyMiner.getNeededColumnMessage(
                        this.getContext().getSelf(), message.getTaskId(), message.getKey1(), message.getKey2(), message.getKey3(), message.getKey4(), true, true);
                this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(requestColumn, message.getDependencyMinerLargeMessageProxy()));
            } else if (!columnOfStrings.containsKey(getCompositeKey(key1, key2))) {
                this.getContext().getLog().info("I am worker {} and I need a column, the keys are {} and {}", this.getContext().getSelf().path().name(), key1, key2);
                LargeMessageProxy.LargeMessage requestColumn = new DependencyMiner.getNeededColumnMessage(
                        this.getContext().getSelf(), message.getTaskId(), key1, key2, null, null, false, true);
                this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(requestColumn, message.getDependencyMinerLargeMessageProxy()));
            } else if (!columnOfStrings.containsKey(getCompositeKey(key3, key4))) {
                this.getContext().getLog().info("I am worker {} and I need a column, the keys are {} and {}", this.getContext().getSelf().path().name(), key3, key4);
                LargeMessageProxy.LargeMessage requestColumn = new DependencyMiner.getNeededColumnMessage(
                        this.getContext().getSelf(), message.getTaskId(), key3, key4, null, null, false, true);
                this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(requestColumn, message.getDependencyMinerLargeMessageProxy()));
            } else
                findingIND();
            //This is if the column is a number column
        } else {
            if (!columnOfNumbers.containsKey(getCompositeKey(key1, key2)) && !columnOfNumbers.containsKey(getCompositeKey(key3, key4))) {
                this.getContext().getLog().info("I am worker {} and I need a column1 and column2, the keys are {} : {} and {} : {}", this.getContext().getSelf().path().name(), key1, key2, key3, key4);
                LargeMessageProxy.LargeMessage requestColumn = new DependencyMiner.getNeededColumnMessage(
                        this.getContext().getSelf(), message.getTaskId(), key1, key2, key3, key4, true, false);
                this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(requestColumn, message.getDependencyMinerLargeMessageProxy()));
            } else if (!columnOfNumbers.containsKey(getCompositeKey(key1, key2))) {
                this.getContext().getLog().info("I am worker {} and I need a column, the keys are {} and {}", this.getContext().getSelf().path().name(), key1, key2);
                LargeMessageProxy.LargeMessage requestColumn = new DependencyMiner.getNeededColumnMessage(
                        this.getContext().getSelf(), message.getTaskId(), key1, key2, null, null, false, false);
                this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(requestColumn, message.getDependencyMinerLargeMessageProxy()));
            } else if (!columnOfNumbers.containsKey(getCompositeKey(key3, key4))) {
                this.getContext().getLog().info("I am worker {} and I need a column, the keys are {} and {}", this.getContext().getSelf().path().name(), key3, key4);
                LargeMessageProxy.LargeMessage requestColumn = new DependencyMiner.getNeededColumnMessage(
                        this.getContext().getSelf(), message.getTaskId(), key3, key4, null, null, false, false);
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
            if (message.column.getType().equals("string")) {
                this.columnOfStrings.put(getCompositeKey(message.getKey1(), message.getKey2()), message.column);
                this.columnOfStrings.put(getCompositeKey(message.getKey3(), message.getKey4()), message.column2);
                this.getContext().getLog().info("I am worker {} and I got a column, the keys are {} and {}", this.getContext().getSelf().path().name(), message.getKey1(), message.getKey2());
                this.getContext().getLog().info("I am worker {} and I got a column, the keys are {} and {}", this.getContext().getSelf().path().name(), message.getKey3(), message.getKey4());
            } else {
                this.columnOfNumbers.put(getCompositeKey(message.getKey1(), message.getKey2()), message.column);
                this.columnOfNumbers.put(getCompositeKey(message.getKey3(), message.getKey4()), message.column2);
                this.getContext().getLog().info("I am worker {} and I got a column, the keys are {} and {}", this.getContext().getSelf().path().name(), message.getKey1(), message.getKey2());
                this.getContext().getLog().info("I am worker {} and I got a column, the keys are {} and {}", this.getContext().getSelf().path().name(), message.getKey3(), message.getKey4());
            }
        } else {
            if (message.column.getType().equals("string")) {
                this.columnOfStrings.put(getCompositeKey(message.getKey1(), message.getKey2()), message.column);
                this.getContext().getLog().info("I am worker {} and I got a column, the keys are {} and {}", this.getContext().getSelf().path().name(), message.getKey1(), message.getKey2());
            } else {
                this.columnOfNumbers.put(getCompositeKey(message.getKey1(), message.getKey2()), message.column);
                this.getContext().getLog().info("I am worker {} and I got a column, the keys are {} and {}", this.getContext().getSelf().path().name(), message.getKey1(), message.getKey2());
            }
        }

        findingIND();

        return this;
    }

    private void findingIND() {

        boolean result;
        Column column1;
        Column column2;
        if (taskMessage.isStringColumn()) {
            column1 = columnOfStrings.get(getCompositeKey(taskMessage.getKey1(), taskMessage.getKey2()));
            column2 = columnOfStrings.get(getCompositeKey(taskMessage.getKey3(), taskMessage.getKey4()));
        } else {
            column1 = columnOfNumbers.get(getCompositeKey(taskMessage.getKey1(), taskMessage.getKey2()));
            column2 = columnOfNumbers.get(getCompositeKey(taskMessage.getKey3(), taskMessage.getKey4()));
        }
        this.getContext().getLog().info("Looking for IND between {} and {}", column1, column2);
        result = column1.getColumnValues().containsAll(column2.getColumnValues());
        this.getContext().getLog().info("{}", result);

        LargeMessageProxy.LargeMessage resultMessage = new DependencyMiner.CompletionMessage(
                this.getContext().getSelf(),
                taskMessage.getTaskId(),
                result,
                column1,
                column2);

        this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(resultMessage, taskMessage.getDependencyMinerLargeMessageProxy()));

    }
}
