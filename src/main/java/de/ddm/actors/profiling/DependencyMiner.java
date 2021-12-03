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
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.InputConfigurationSingleton;
import de.ddm.singletons.SystemConfigurationSingleton;
import de.ddm.structures.InclusionDependency;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

import java.io.File;
import java.util.*;

@Slf4j
public class DependencyMiner extends AbstractBehavior<DependencyMiner.Message> {

    // Local state
    Map<Integer, Pair<Integer, Integer>> taskToFileidMap = new HashMap<>();
    Map<Integer, List<Set<String>>> fileidToContentMap = new HashMap<>();
    Map<Integer, Boolean> filereadCompletionMap = new HashMap<>();
    Map<Integer, Boolean> taskCompletionMap = new HashMap<>();
    int nextTaskId = 0;

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
        int task;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CompletionMessage implements Message {
        private static final long serialVersionUID = -7642425159675583598L;
        ActorRef<DependencyWorker.Message> dependencyWorker;
        List<Integer[]> dependencies;
        int taskId;
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

        context.getSystem().receptionist().tell(Receptionist.register(dependencyMinerService, context.getSelf()));
    }

    /////////////////
    // Actor State //
    /////////////////

    private long startTime;

    private final boolean discoverNaryDependencies;
    private final File[] inputFiles;
    private final String[][] headerLines;

    private final List<ActorRef<InputReader.Message>> inputReaders;
    private final ActorRef<ResultCollector.Message> resultCollector;
    private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;

    private final List<ActorRef<DependencyWorker.Message>> dependencyWorkers;

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
                Set<String> column = new HashSet<>();
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
        }

        if (!filereadCompletionMap.values().stream().allMatch((v) -> v)) {
            return this;
        }

        this.getContext().getLog().info("Finished reading the input");

        int taskCounter = 0;
        for (int i = 0; i < filereadCompletionMap.size(); i++) {
            for (int j = 0; j < filereadCompletionMap.size(); j++) {
                if (i == j)
                    continue;

                Pair<Integer, Integer> filePair = Pair.of(i, j);
                taskToFileidMap.put(taskCounter, filePair);
                this.taskCompletionMap.put(taskCounter, false);
                taskCounter++;
            }
        }

        for (ActorRef<DependencyWorker.Message> dependencyWorker : this.dependencyWorkers) {
            dependencyWorker.tell(new DependencyWorker.TaskMessage(getContext().getSelf(), this.nextTaskId++));
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
            // TODO handle the case if the worker join after the input is completely read

            if (!filereadCompletionMap.values().stream().allMatch((v) -> v)) {
                return this;
            }
            if (this.nextTaskId < this.taskToFileidMap.size()) {
                dependencyWorker.tell(new DependencyWorker.TaskMessage(getContext().getSelf(), this.nextTaskId++));
            }

            //dependencyWorker.tell(new DependencyWorker.TaskMessage(this.largeMessageProxy, 42));
        }
        return this;
    }

    private Behavior<Message> handle(CompletionMessage message) {
        ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
        // If this was a reasonable result, I would probably do something with it and potentially generate more work
        // ... for now, let's just generate a random, binary IND.

        List<InclusionDependency> inds = new ArrayList<>(1);

        List<Integer[]> dependencies = message.dependencies;
        int taskId = message.taskId;
        Pair<Integer, Integer> fileIdPair = taskToFileidMap.get(taskId);

        taskCompletionMap.put(message.taskId, true);

        File referencedFile = this.inputFiles[fileIdPair.getLeft()];
        File dependentFile = this.inputFiles[fileIdPair.getRight()];

        for (Integer[] dependency : dependencies) {
            String referencedAttribute = this.headerLines[fileIdPair.getLeft()][dependency[0]];
            String dependentAttribute = this.headerLines[fileIdPair.getRight()][dependency[1]];

            InclusionDependency ind = new InclusionDependency(dependentFile, new String[]{dependentAttribute},
                    referencedFile, new String[]{referencedAttribute});

            inds.add(ind);
        }

        this.resultCollector.tell(new ResultCollector.ResultMessage(inds));

        // I still don't know what task the worker could help me to solve ... but let me keep her busy.
        // Once I found all unary INDs, I could check if this.discoverNaryDependencies is set to true and try to
        // detect n-ary INDs as well!
        if (this.nextTaskId < this.taskToFileidMap.size()) {
            dependencyWorker.tell(new DependencyWorker.TaskMessage(getContext().getSelf(), this.nextTaskId++));
        }

        // End worker when all tasks are done
        if (this.taskCompletionMap.values().stream().allMatch((v) -> v)) {

            this.end();
        }

        return this;
    }

    private Behavior<Message> handle(RequestDataMessage message) {
        ActorRef<LargeMessageProxy.Message> receiverProxy = message.dependencyWorkerReceiverProxy;

        this.getContext().getLog().info("Requesting Data {}", receiverProxy.toString());

        int taskId = message.task;
        Pair<Integer, Integer> fileIds = taskToFileidMap.get(taskId);
        List<Set<String>> file1 = fileidToContentMap.get(fileIds.getLeft());
        List<Set<String>> file2 = fileidToContentMap.get(fileIds.getRight());

        this.getContext().getLog().info("columns of file 1: {}", file1.size());
        this.getContext().getLog().info("columns of file 2: {}", file2.size());

        LargeMessageProxy.LargeMessage largeMessage = new DependencyWorker.DataMessage(file1, file2,
                this.largeMessageProxy, taskId);
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
        this.dependencyWorkers.remove(dependencyWorker);
        return this;
    }

	private Behavior<Message> handle(ShutdownMessage message) {
		return Behaviors.stopped();
	}
}