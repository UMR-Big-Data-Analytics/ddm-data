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
import java.util.Arrays;
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
        int referencedFileId;
        int dependentFileId;
        int dependentFileColumnIndex;
        int dependentColumnFrom;
        int dependentColumnTo;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DataMessage implements Message {
        private static final long serialVersionUID = 2135984614102497577L;
        String[][] referencedFile;
        String[] maybeDependentColumnPart;
        ActorRef<LargeMessageProxy.Message> dependencyMinerLargeMessageProxy;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ProcessDataMessage implements Message {
        private static final long serialVersionUID = -5292648222781068512L;
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

    private String[][] referencedFile = null;
    private int nextReferencedColumnId = 0;
    private String[] dependentColumnPart = null;
    private int referencedFileId = -1;
    private int dependentFileId = -1;
    private int dependentFileColumnIndex = -1;
    List<Integer[]> dependencies = new ArrayList<>();

    ////////////////////
    // Actor Behavior //
    ////////////////////

    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(ReceptionistListingMessage.class, this::handle)
                .onMessage(TaskMessage.class, this::handle)
                .onMessage(DataMessage.class, this::handle)
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

        boolean needReferencedFileData = message.referencedFileId != this.referencedFileId;
        this.referencedFileId = message.referencedFileId;
        this.dependentFileId = message.dependentFileId;
        this.dependentFileColumnIndex = message.dependentFileColumnIndex;

        this.nextReferencedColumnId = 0;
        this.dependencies = new ArrayList<>();
        this.referencedColumnCandidates = new ArrayList<>();
        // Request data from dependency miner
        getContext().getLog().info("TaskMessage: referencedFileId: " + this.referencedFileId + ", dependentFileId:" + this.dependentFileId + ", dependentFileColumnIndex: " + this.dependentFileColumnIndex);
        message.dependencyMiner.tell(new DependencyMiner.RequestDataMessage(this.largeMessageProxy,
                needReferencedFileData ? this.referencedFileId : -1, this.dependentFileId,
                this.dependentFileColumnIndex, message.dependentColumnFrom, message.dependentColumnTo));

        return this;
    }

    List<Integer> referencedColumnCandidates;

    private Behavior<Message> handle(ProcessDataMessage message) {
        String[] columnOfFile1 = this.referencedFile[this.nextReferencedColumnId];
        //this.getContext().getLog().info("check {} <- {} ?", this.nextReferencedColumnId, this.dependentFileColumnIndex);


        boolean isDependencyCandidate = true;
        for (String s : this.dependentColumnPart) {
            if (Arrays.binarySearch(columnOfFile1, s) < 0) {
                isDependencyCandidate = false;
                break;
            }
        }

        if (isDependencyCandidate) {
            referencedColumnCandidates.add(this.nextReferencedColumnId);
        }

        this.nextReferencedColumnId += 1;
        if (this.nextReferencedColumnId >= this.referencedFile.length) {
            LargeMessageProxy.LargeMessage completionMessage =
                    new DependencyMiner.CompletionMessage(this.getContext().getSelf(), this.dependentFileId,
                            this.referencedFileId, this.dependentFileColumnIndex, this.referencedColumnCandidates);

            this.getContext().getLog().info("column candidates {}", this.referencedColumnCandidates.toString());

            this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(completionMessage,
                    message.getDependencyMinerLargeMessageProxy()));
        } else {
            getContext().getSelf().tell(new ProcessDataMessage(message.dependencyMinerLargeMessageProxy));
        }

        return this;
    }

    private Behavior<Message> handle(DataMessage message) {
        this.getContext().getLog().info("Working!");

        if (message.referencedFile != null) {
            this.referencedFile = message.referencedFile;
        }
        this.dependentColumnPart = message.maybeDependentColumnPart;

        getContext().getSelf().tell(new ProcessDataMessage(message.dependencyMinerLargeMessageProxy));

        return this;
    }

}
