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
import de.ddm.structures.CandidatePair;
import de.ddm.structures.Column;
import de.ddm.structures.InclusionDependency;
import de.ddm.structures.Table;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

import static java.lang.Thread.sleep;

public class DependencyMiner extends AbstractBehavior<DependencyMiner.Message> {

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
        List<Column> batch;
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
    public static class CompletionMessage implements Message {
        private static final long serialVersionUID = -7642425159675583598L;
        ActorRef<DependencyWorker.Message> dependencyWorker;
        int result;

        int firstTableIndex;
        String firstColumnName;

        int secondTableIndex;
        String secondColumnName;
    }

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "dependencyMiner";

    public static final ServiceKey<DependencyMiner.Message> dependencyMinerService = ServiceKey.create(DependencyMiner.Message.class, DEFAULT_NAME + "Service");

    public static Behavior<Message> create() {
        return Behaviors.setup(DependencyMiner::new);
    }

    private DependencyMiner(ActorContext<Message> context) {
        super(context);
        this.discoverNaryDependencies = SystemConfigurationSingleton.get().isHardMode();
        this.inputFiles = InputConfigurationSingleton.get().getInputFiles();
        this.tables = new ArrayList<Table>();
        this.taskQueue = new LinkedList<>();
        this.taskTracking = new HashMap<>();
        this.inputReaders = new ArrayList<>(inputFiles.length);
        for (int id = 0; id < this.inputFiles.length; id++)
            this.inputReaders.add(context.spawn(InputReader.create(id, this.inputFiles[id]), InputReader.DEFAULT_NAME + "_" + id));
        this.resultCollector = context.spawn(ResultCollector.create(), ResultCollector.DEFAULT_NAME);
        this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);

        this.dependencyWorkers = new ArrayList<>();

        context.getSystem().receptionist().tell(Receptionist.register(dependencyMinerService, context.getSelf()));
    }

    /////////////////
    // Actor State //
    /////////////////

    private long startTime;

    private final boolean discoverNaryDependencies;
    private final File[] inputFiles;
    private List<Table> tables;
    private Queue<CandidatePair> taskQueue;
    private HashMap<ActorRef<DependencyWorker.Message>, CandidatePair> taskTracking;
    private int taskCounter = 0;
    private boolean finishedFillingQueue = false;
    private final List<ActorRef<InputReader.Message>> inputReaders;
    private final ActorRef<ResultCollector.Message> resultCollector;
    private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;

    private final List<ActorRef<DependencyWorker.Message>> dependencyWorkers;

    private int currentWorkerIndex = 0;

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
        Table table = new Table(message.getId());
        table.setColumns(Arrays.stream(message.getHeader())
                .map(header -> new Column(message.getId(), header))
                .collect(Collectors.toList()));
        this.tables.add(table);

        checkAndStartTaskDistribution();
        return this;
    }

    private Behavior<Message> handle(BatchMessage message) {
        Table table = this.tables.stream().filter(t -> t.getId() == message.getId()).findFirst().orElse(null);
        assert table != null;
        if (!message.getBatch().isEmpty()) {

            List<Column> columns = table.getColumns();
            List<Column> addedColumns = new ArrayList<>(columns.size());
            for (Column column : columns) {
                Column batchColumn = message.getBatch().stream().filter(c -> c.getName().equals(column.getName())).findFirst().orElse(null);
                if (batchColumn == null)
                    throw new IllegalArgumentException("Batch column is null");
                column.getValues().addAll(batchColumn.getValues());
                addedColumns.add(column);
            }
            table.setColumns(addedColumns);
            this.inputReaders.get(message.getId()).tell(new InputReader.ReadBatchMessage(this.getContext().getSelf()));
        }
        else {
            table.setFullyLoaded(true);
            this.getContext().getLog().info("Finished reading table {} with {} columns", message.getId(), table.getColumns().size());
        }

        checkAndStartTaskDistribution();
        return this;
    }

    private Behavior<Message> handle(RegistrationMessage message) {
        ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
        if (!this.dependencyWorkers.contains(dependencyWorker)) {
            this.dependencyWorkers.add(dependencyWorker);
            this.getContext().watch(dependencyWorker);

            distributeNextTask(dependencyWorker);
        }
        return this;
    }

    private Behavior<Message> handle(CompletionMessage message) {
        ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
        taskTracking.remove(dependencyWorker);

        if (message.getResult() == -1) {
            this.taskQueue.add(new CandidatePair(message.getFirstTableIndex(), message.getFirstColumnName(), message.getSecondTableIndex(), message.getSecondColumnName()));
            getContext().getLog().warn("Result of the worker was -1");
        } else if (message.getResult() == 1) {
            Table table = this.tables.stream().filter(t -> t.getId() == message.getFirstTableIndex()).findFirst().orElse(null);
            assert table != null;
            InclusionDependency ind = generateDependency(message);
            table.getDependencies().add(ind);
            this.resultCollector.tell(new ResultCollector.ResultMessage(Collections.singletonList(ind)));
        }

        distributeNextTask(dependencyWorker);
        return this;
    }

    private static boolean areAllTrue(List<Boolean> array) {
        for (boolean b : array) if (!b) return false;
        return true;
    }

    private InclusionDependency generateDependency(CompletionMessage message) {
        File dependentFile = this.inputFiles[message.getFirstTableIndex()];
        File referencedFile = this.inputFiles[message.getSecondTableIndex()];
        return new InclusionDependency(dependentFile, new String[]{message.getFirstColumnName()}, referencedFile, new String[]{message.getSecondColumnName()});
    }

    private void checkAndStartTaskDistribution() {
        if (areAllTrue(tables.stream().map(Table::isFullyLoaded).collect(Collectors.toList()))) {
            getContext().getLog().info("All headers and batches loaded, starting task distribution.");
            generateUnaryTasks();
            for (ActorRef<DependencyWorker.Message> dependencyWorker : this.dependencyWorkers)
                distributeNextTask(dependencyWorker);
        }
    }

    private void end() {
        this.resultCollector.tell(new ResultCollector.FinalizeMessage());
        long discoveryTime = System.currentTimeMillis() - this.startTime;
        this.getContext().getLog().info("Finished mining within {} ms!", discoveryTime);
    }

    private Behavior<Message> handle(Terminated signal) {
        ActorRef<DependencyWorker.Message> dependencyWorker = signal.getRef().unsafeUpcast();
        this.dependencyWorkers.remove(dependencyWorker);
        if (taskTracking.containsKey(dependencyWorker)){
            taskQueue.add(taskTracking.get(dependencyWorker));
        }
        return this;
    }

    private void generateUnaryTasks() {
        for (int i = 0; i < tables.size(); i++) {
            for (int j = 0; j < tables.get(i).getColumns().size(); j++) {
                for (int k = 0; k < tables.size(); k++) {
                    for (int l = 0; l < tables.get(k).getColumns().size(); l++) {
                        if (!(i == k && j == l)) {
                            Column firstColumn = tables.get(i).getColumns().get(j);
                            Column secondColumn = tables.get(k).getColumns().get(l);
                            taskQueue.add(new CandidatePair(i, firstColumn.getName(), k, secondColumn.getName()));
                        }
                    }
                }
            }
        }
        finishedFillingQueue = true;
    }

    private void distributeNextTask(ActorRef<DependencyWorker.Message> worker) {
        if(worker == null)
            throw new IllegalArgumentException("Worker is null");

        if (finishedFillingQueue && taskQueue.isEmpty()) {
            end();
            return;
        }

        if (!finishedFillingQueue && taskQueue.isEmpty()){
            this.getContext().getLog().warn("No tasks have been generated yet!");
            return;
        }

        CandidatePair pair = taskQueue.poll();
        if (pair != null) {

            Column firstColumn = tables.get(pair.getFirstTableIndex()).getColumns().stream().filter(c -> c.getName().equals(pair.getFirstColumnName())).findFirst().orElse(null);
            Column secondColumn = tables.get(pair.getSecondTableIndex()).getColumns().stream().filter(c -> c.getName().equals(pair.getSecondColumnName())).findFirst().orElse(null);
            DependencyWorker.TaskMessage task = new DependencyWorker.TaskMessage(
                    largeMessageProxy,
                    firstColumn,
                    secondColumn,
                    taskCounter++);
            this.taskTracking.put(worker, pair);
            worker.tell(task);
            this.getContext().getLog().info(" task {} sent to worker", taskCounter);

        }

    }
}
