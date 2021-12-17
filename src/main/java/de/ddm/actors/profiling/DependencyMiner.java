package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.Terminated;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import de.ddm.actors.ColumnId;
import de.ddm.actors.TaskFactory;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.InputConfigurationSingleton;
import de.ddm.singletons.SystemConfigurationSingleton;
import de.ddm.structures.InclusionDependency;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.*;

@Slf4j
public class DependencyMiner extends AbstractBehavior<DependencyMiner.Message> {

    // Local state
    Map<Integer, List<Set<String>>> fileidToContentMap = new HashMap<>();
    Map<Integer, Boolean> filereadCompletionMap = new HashMap<>();
    Map<ActorRef<DependencyWorker.Message>, ColumnId> actorRefToColumnMap = new HashMap<>();
    Map<ActorRef<DependencyWorker.Message>, DependencyWorker.TaskMessage> actorRefToActorOccupationMap =
            new HashMap<>();

    String[][][] alternativeFileRepresentation;

    ////////////////////
    // Actor Messages //
    ////////////////////

    public interface Message extends AkkaSerializable, LargeMessageProxy.LargeMessage {
    }

    @NoArgsConstructor
    public static class StartMessage implements Message {
        private static final long serialVersionUID = -1963913294517850454L;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class HeaderMessage implements Message {
        private static final long serialVersionUID = -5322425954432915838L;
        int id;
        String[] header;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class BatchMessage implements Message {
        private static final long serialVersionUID = 4591192372652568030L;
        int id;
        List<String[]> batch;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class RegistrationMessage implements Message {
        private static final long serialVersionUID = -4025238529984914107L;
        ActorRef<DependencyWorker.Message> dependencyWorker;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class RequestDataMessage implements Message {
        private static final long serialVersionUID = 868083729453247423L;
        ActorRef<LargeMessageProxy.Message> dependencyWorkerReceiverProxy;
        ColumnId referencedColumnId;
        ColumnId dependentColumnId;
        int dependentColumnFrom;
        int dependentColumnTo;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CompletionMessage implements Message {
        private static final long serialVersionUID = -7642425159675583598L;
        ActorRef<DependencyWorker.Message> dependencyWorker;
        ColumnId referencedColumnId;
        ColumnId dependentColumnId;
        boolean isCandidate;
    }

    @NoArgsConstructor
    public static class ShutdownMessage implements Message {
        private static final long serialVersionUID = -905237396060190564L;
    }

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "dependencyMiner";

    public static final ServiceKey<DependencyMiner.Message> dependencyMinerService =
            ServiceKey.create(DependencyMiner.Message.class, DEFAULT_NAME + "Service");

    public static Behavior<Message> create() {
        return Behaviors.setup(DependencyMiner::new);
    }

    private DependencyMiner(ActorContext<Message> context) {
        super(context);
        this.discoverNaryDependencies = SystemConfigurationSingleton.get().isHardMode();
        this.inputFiles = InputConfigurationSingleton.get().getInputFiles();
        this.headerLines = new String[this.inputFiles.length][];

        this.alternativeFileRepresentation = new String[this.inputFiles.length][][];

        this.taskFactory = new TaskFactory(this.getContext().getSelf(), alternativeFileRepresentation);

        this.inputReaders = new ArrayList<>(inputFiles.length);
        for (int id = 0; id < this.inputFiles.length; id++) {
            this.inputReaders.add(context.spawn(InputReader.create(id, this.inputFiles[id]),
                    InputReader.DEFAULT_NAME + "_" + id));
            this.filereadCompletionMap.put(id, false);
            fileidToContentMap.put(id, new ArrayList<>());
        }

        this.resultCollector = context.spawn(ResultCollector.create(), ResultCollector.DEFAULT_NAME);
        this.largeMessageProxy =
                this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()),
                        LargeMessageProxy.DEFAULT_NAME);

        this.dependencyWorkers = new ArrayList<>();

        this.unemployedDependencyWorkers = new ArrayList<>();

        context.getSystem().receptionist().tell(Receptionist.register(dependencyMinerService, context.getSelf()));
    }

    /////////////////
    // Actor State //
    /////////////////

    private long startTime;

    private final boolean discoverNaryDependencies;
    private final File[] inputFiles;
    private final String[][] headerLines;

    private TaskFactory taskFactory;

    private final List<ActorRef<InputReader.Message>> inputReaders;
    private final ActorRef<ResultCollector.Message> resultCollector;
    private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;

    private final List<ActorRef<DependencyWorker.Message>> dependencyWorkers;

    private final List<ActorRef<DependencyWorker.Message>> unemployedDependencyWorkers;

    ////////////////////
    // Actor Behavior //
    ////////////////////

    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(StartMessage.class, this::handle)
                .onMessage(BatchMessage.class, this::handle)
                .onMessage(HeaderMessage.class, this::handle)
                .onMessage(RegistrationMessage.class, this::handle)
                .onMessage(CompletionMessage.class, this::handle)
                .onMessage(RequestDataMessage.class, this::handle)
                .onMessage(ShutdownMessage.class, this::handle)
                .onSignal(Terminated.class, this::handle)
                .build();
    }

    private Behavior<Message> handle(StartMessage message) {
        for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
            inputReader.tell(new InputReader.ReadHeaderMessage(this.getContext().getSelf()));
        for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
            inputReader.tell(new InputReader.ReadBatchMessage(this.getContext().getSelf()));
        this.startTime = System.currentTimeMillis();
        return this;
    }

    private Behavior<Message> handle(HeaderMessage message) {
        this.headerLines[message.getId()] = message.getHeader();
        return this;
    }

    private Behavior<Message> handle(BatchMessage message) {
        // Ignoring batch content for now ... but I could do so much with it.
        this.getContext().getLog().info("BatchMessage of file {}", message.id);
        if (message.getBatch().size() != 0) {
            for (int i = 0; i < message.batch.get(0).length; i++) {
                Set<String> column = new TreeSet<>();
                for (int j = 0; j < message.batch.size(); j++) {
                    column.add(message.batch.get(j)[i]);
                }
                List<Set<String>> columnList = fileidToContentMap.get(message.id);
                if (i < columnList.size()) {
                    columnList.get(i).addAll(column);
                } else {
                    columnList.add(column);
                }
            }


            this.inputReaders.get(message.getId()).tell(new InputReader.ReadBatchMessage(this.getContext().getSelf()));
        } else {
            filereadCompletionMap.put(message.id, true);

            List<Set<String>> columns = fileidToContentMap.get(message.id);
            alternativeFileRepresentation[message.id] = new String[columns.size()][];
            for (int i = 0; i < columns.size(); i++) {
                String[] column = columns.get(i).toArray(new String[0]);
                Arrays.sort(column);
                alternativeFileRepresentation[message.id][i] = column;
            }


            this.getContext().getLog().info("File {} complete", message.id);
            this.taskFactory.addFile(message.id, this.fileidToContentMap.get(message.id).size());
        }

        for (ActorRef<DependencyWorker.Message> dependencyWorker : this.dependencyWorkers) {
            if (this.taskFactory.hasWork() && this.actorRefToActorOccupationMap.get(dependencyWorker) == null) {
                ColumnId referencedColumnId = this.taskFactory.nextReferencedColumnId();

                this.actorRefToColumnMap.put(dependencyWorker, referencedColumnId);
                if (this.taskFactory.hasNextTaskByReferencedColumn(referencedColumnId)) {
                    DependencyWorker.TaskMessage taskMessage =
                            this.taskFactory.nextTaskByReferencedFile(referencedColumnId);
                    this.actorRefToActorOccupationMap.put(dependencyWorker, taskMessage);
                    dependencyWorker.tell(taskMessage);
                }
            }
        }

        return this;
    }

    private Behavior<Message> handle(RegistrationMessage message) {
        ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
        if (!this.dependencyWorkers.contains(dependencyWorker)) {
            this.dependencyWorkers.add(dependencyWorker);
            this.getContext().watch(dependencyWorker);
            // The worker should get some work ... let me send her something before I figure out what I actually want
            // from her.
            // I probably need to idle the worker for a while, if I do not have work for it right now ... (see
            // master/worker pattern)

            this.actorRefToActorOccupationMap.put(dependencyWorker, null);

            if (this.taskFactory.hasWork()) {
                ColumnId referencedColumnId = this.taskFactory.nextReferencedColumnId();
                this.actorRefToColumnMap.put(dependencyWorker, referencedColumnId);
                if (this.taskFactory.hasNextTaskByReferencedColumn(referencedColumnId)) {
                    DependencyWorker.TaskMessage taskMessage =
                            this.taskFactory.nextTaskByReferencedFile(referencedColumnId);
                    this.actorRefToActorOccupationMap.put(dependencyWorker, taskMessage);
                    dependencyWorker.tell(taskMessage);
                }
            }
        }

        return this;
    }

    private Behavior<Message> handle(CompletionMessage message) {
        ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
        // If this was a reasonable result, I would probably do something with it and potentially generate more work
        // ... for now, let's just generate a random, binary IND.

        InclusionDependency ind = this.taskFactory.handleCompletionMessage(message,
                (ColumnId referencedColumnId, ColumnId dependentColumnId) -> {
                    File referencedFile = this.inputFiles[referencedColumnId.getFileId()];
                    File dependentFile = this.inputFiles[dependentColumnId.getFileId()];

                    String referencedAttribute = this.headerLines[referencedColumnId.getFileId()][referencedColumnId.getColumnId()];
                    String dependentAttribute = this.headerLines[dependentColumnId.getFileId()][dependentColumnId.getColumnId()];

                    return new InclusionDependency(dependentFile, new String[]{dependentAttribute},
                            referencedFile, new String[]{referencedAttribute});
                });

        getContext().getLog().info("CompletionMessage: dependentColumnId: " + message.dependentColumnId.toString());

        if (ind != null) {
            this.resultCollector.tell(new ResultCollector.ResultMessage(Collections.singletonList(ind)));
        }

        // I still don't know what task the worker could help me to solve ... but let me keep her busy.
        // Once I found all unary INDs, I could check if this.discoverNaryDependencies is set to true and try to
        // detect n-ary INDs as well!
        ColumnId referencedColumnId = this.actorRefToColumnMap.get(dependencyWorker);
        if (this.taskFactory.hasNextTaskByReferencedColumn(referencedColumnId)) {
            dependencyWorker.tell(this.taskFactory.nextTaskByReferencedFile(referencedColumnId));
            return this;
        } else if (this.taskFactory.hasWork()) {
            ColumnId newReferencedColumnId = this.taskFactory.nextReferencedColumnId();
            this.actorRefToColumnMap.put(dependencyWorker, newReferencedColumnId);
            if (this.taskFactory.hasNextTaskByReferencedColumn(newReferencedColumnId)) {
                DependencyWorker.TaskMessage taskMessage =
                        this.taskFactory.nextTaskByReferencedFile(newReferencedColumnId);
                this.actorRefToActorOccupationMap.put(dependencyWorker, taskMessage);
                dependencyWorker.tell(taskMessage);
            }
            return this;
        } else {
            this.actorRefToActorOccupationMap.put(dependencyWorker, null);
        }

        if (this.filereadCompletionMap.values().stream().allMatch((v) -> v)
                && this.actorRefToActorOccupationMap.values().stream().allMatch(Objects::isNull)
                && !this.taskFactory.hasWork()) {
            this.end();
        }

        return this;
    }

    private Behavior<Message> handle(RequestDataMessage message) {
        ActorRef<LargeMessageProxy.Message> receiverProxy = message.dependencyWorkerReceiverProxy;

        this.getContext().getLog().info("Requesting Data {}", receiverProxy.toString());

        String[] referencedColumn = message.referencedColumnId == null ? null :
                this.alternativeFileRepresentation[message.getReferencedColumnId().getFileId()][message.getReferencedColumnId().getColumnId()];
        String[] maybeDependentColumnPart =
                Arrays.copyOfRange(this.alternativeFileRepresentation[message.getDependentColumnId().getFileId()][message.getDependentColumnId().getColumnId()],
                        message.dependentColumnFrom, message.dependentColumnTo);

        LargeMessageProxy.LargeMessage largeMessage = new DependencyWorker.DataMessage(referencedColumn,
                maybeDependentColumnPart,
                this.largeMessageProxy);
        this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(largeMessage, receiverProxy));

        return this;
    }

    private void end() {
        this.resultCollector.tell(new ResultCollector.FinalizeMessage());
        long discoveryTime = System.currentTimeMillis() - this.startTime;
        this.getContext().getLog().info("Finished mining within {} ms!", discoveryTime);
    }

    private Behavior<Message> handle(Terminated signal) {
        ActorRef<DependencyWorker.Message> dependencyWorker = signal.getRef().unsafeUpcast();
        this.getContext().getLog().error("Actor {} has terminated", dependencyWorker);
        DependencyWorker.TaskMessage taskMessage = this.actorRefToActorOccupationMap.remove(dependencyWorker);
        this.actorRefToColumnMap.remove(dependencyWorker);
        this.dependencyWorkers.remove(dependencyWorker);

        boolean addToTaskFactory = true;
        for (ActorRef<DependencyWorker.Message> worker : this.dependencyWorkers) {
            if (this.actorRefToActorOccupationMap.get(worker) == null) {
                worker.tell(taskMessage);
                addToTaskFactory = false;
                break;
            }
        }
        if (addToTaskFactory) {
            this.taskFactory.addFailedTask(taskMessage);
        }
        return this;
    }

    private Behavior<Message> handle(ShutdownMessage message) {
        return Behaviors.stopped();
    }
}