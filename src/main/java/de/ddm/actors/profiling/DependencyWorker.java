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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

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
        int task;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DataMessage implements Message {
        private static final long serialVersionUID = 2135984614102497577L;
        List<Set<String>> file1;
        List<Set<String>> file2;
        ActorRef<LargeMessageProxy.Message> dependencyMinerLargeMessageProxy;
        int task;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ProcessDataMessage implements Message {
        private static final long serialVersionUID = -5292648222781068512L;
        ActorRef<LargeMessageProxy.Message> dependencyMinerLargeMessageProxy;
        int task;
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

    List<Set<String>> file1;
    List<Set<String>> file2;
    int file1Index;
    int file2Index;

    ////////////////////
    // Actor Behavior //
    ////////////////////

    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(ReceptionistListingMessage.class, this::handle)
                .onMessage(TaskMessage.class, this::handle)
                .onMessage(DataMessage.class, this::handle)
                .onMessage(ShutdownMessage.class, this::handle)
                .onMessage(ProcessDataMessage.class, this::handle)
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

        // Request data from dependency miner
        message.dependencyMiner.tell(new DependencyMiner.RequestDataMessage(this.largeMessageProxy, message.task));

        return this;
    }

    private Behavior<Message> handle(ProcessDataMessage message) {
        List<Integer[]> dependencies = new ArrayList<>();

        Set<String> columnOfFile1 = file1.get(file1Index);
        Set<String> columnOfFile2 = file2.get(file2Index);
        if (columnOfFile1.containsAll(columnOfFile2)) {
            dependencies.add(new Integer[]{file1Index, file2Index});
        }

        file2Index++;
        if(file2Index == file2.size()) {
            file2Index = 0;
            file1Index++;
        }

        if(file1Index != file1.size()) {
            this.getContext().getSelf().tell(new ProcessDataMessage(message.dependencyMinerLargeMessageProxy, message.task));
            return this;
        }

        LargeMessageProxy.LargeMessage completionMessage =
                new DependencyMiner.CompletionMessage(this.getContext().getSelf(), dependencies, message.task);
        this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(completionMessage,
                message.getDependencyMinerLargeMessageProxy()));

        return this;
    }

    private Behavior<Message> handle(DataMessage message) {
        this.getContext().getLog().info("Working!");

        file1 = message.file1;
        file2 = message.file2;
        file1Index = 0;
        file2Index = 0;

        this.getContext().getSelf().tell(new ProcessDataMessage(message.dependencyMinerLargeMessageProxy, message.task));

        return this;
    }

    private Behavior<Message> handle(ShutdownMessage message) {
        return Behaviors.stopped();
    }

}
