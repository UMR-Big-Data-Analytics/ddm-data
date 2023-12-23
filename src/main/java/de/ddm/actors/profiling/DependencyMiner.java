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

import java.io.File;
import java.util.*;

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
        List<String[]> batch;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class RegistrationMessage implements Message {
        private static final long serialVersionUID = -4025238529984914107L;
        ActorRef<DependencyWorker.Message> dependencyWorker;
        ActorRef<LargeMessageProxy.Message> dependencyMinerLargeMessageProxy;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class getNeededColumnMessage implements Message {
        private static final long serialVersionUID = -4025238529984914107L;
        ActorRef<DependencyWorker.Message> dependencyWorker;
        int taskId;
        int key1;
        int key2;
        boolean getBothColumns;
        boolean isString;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CompletionMessage implements Message {
        private static final long serialVersionUID = -7642425159675583598L;
        ActorRef<DependencyWorker.Message> dependencyWorker;
        int taskId;
        boolean hasDependency;
        int key1;
        int key2;
        boolean isString;
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
        this.headerLines = new String[this.inputFiles.length][];

        this.inputReaders = new ArrayList<>(inputFiles.length);
        for (int id = 0; id < this.inputFiles.length; id++)
            this.inputReaders.add(context.spawn(InputReader.create(id, this.inputFiles[id]), InputReader.DEFAULT_NAME + "_" + id));
        this.resultCollector = context.spawn(ResultCollector.create(), ResultCollector.DEFAULT_NAME);
        this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);
        this.dependencyWorkersLargeMessageProxy = new ArrayList<>();
        this.dependencyWorkers = new ArrayList<>();
        this.allFilesHaveBeenRead = new boolean[this.inputFiles.length];
        context.getSystem().receptionist().tell(Receptionist.register(dependencyMinerService, context.getSelf()));
    }

    /////////////////
    // Actor State //
    /////////////////

    private long startTime;

    private List<Boolean> taskDone = new ArrayList<>();
    private final boolean discoverNaryDependencies;
    private final File[] inputFiles;
    private final String[][] headerLines;
    private final boolean[] allFilesHaveBeenRead;
    private final List<ActorRef<LargeMessageProxy.Message>> dependencyWorkersLargeMessageProxy;
    // CompositeKey the first key is the name of the file and the second key is the name of the column
    private final HashMap<Integer, Column> columnOfStrings = new HashMap<>();
    private final HashMap<Integer, Column> columnOfNumbers = new HashMap<>();
    private final HashMap<String, Integer> keyDictionary = new HashMap<>();
    private final List<DependencyWorker.TaskMessage> listOfTasks = new ArrayList<>();
    private boolean allTasksHavebeenCreated = false;
    private int currentTask = 0;
    private ActorRef<DependencyWorker.Message> lastWorker;
    private final List<ActorRef<InputReader.Message>> inputReaders;
    private final ActorRef<ResultCollector.Message> resultCollector;
    private final List<ActorRef<DependencyWorker.Message>> dependencyWorkers;
    private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;


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
                .onMessage(getNeededColumnMessage.class, this::handle)
                .onSignal(Terminated.class, this::handle)
                .build();
    }

    // This is for the dependencyWorker to ask for a column
    private Behavior<Message> handle(getNeededColumnMessage getNeededColumnMessage) {
        if (getNeededColumnMessage.getBothColumns) {
            if (getNeededColumnMessage.isString) {
                this.getContext().getLog().info("Received getNeededColumnMessage from worker {} for task {}!", getNeededColumnMessage.getDependencyWorker(), getNeededColumnMessage.getTaskId());
                getNeededColumnMessage.
                        getDependencyWorker().
                        tell(new DependencyWorker.
                                ColumnReceiver(getNeededColumnMessage.getTaskId(), true, columnOfStrings.get(getNeededColumnMessage.getKey1())
                                , getNeededColumnMessage.getKey1(), getNeededColumnMessage.getKey2(), columnOfStrings.get(getNeededColumnMessage.getKey2())));
            } else {
                this.getContext().getLog().info("Received getNeededColumnMessage from worker {} for task {}!", getNeededColumnMessage.getDependencyWorker(), getNeededColumnMessage.getTaskId());
                getNeededColumnMessage.
                        getDependencyWorker().
                        tell(new DependencyWorker.
                                ColumnReceiver(getNeededColumnMessage.getTaskId(), true, columnOfNumbers.get(getNeededColumnMessage.getKey1())
                                , getNeededColumnMessage.getKey1(), getNeededColumnMessage.getKey2(), columnOfNumbers.get(getNeededColumnMessage.getKey2())));
            }
        } else if (getNeededColumnMessage.isString) {
            this.getContext().getLog().info("Received getNeededColumnMessage from worker {} for task {}!", getNeededColumnMessage.getDependencyWorker(), getNeededColumnMessage.getTaskId());
            getNeededColumnMessage.
                    getDependencyWorker().
                    tell(new DependencyWorker.
                            ColumnReceiver(getNeededColumnMessage.getTaskId(), false, columnOfStrings.get(getNeededColumnMessage.getKey1())
                            , getNeededColumnMessage.getKey1(), -1, null));

        } else {
            this.getContext().getLog().info("Received getNeededColumnMessage from worker {} for task {}!", getNeededColumnMessage.getDependencyWorker(), getNeededColumnMessage.getTaskId());
            getNeededColumnMessage.getDependencyWorker()
                    .tell(new DependencyWorker.
                            ColumnReceiver(getNeededColumnMessage.getTaskId(), false, columnOfNumbers.get(getNeededColumnMessage.getKey1())
                            , getNeededColumnMessage.getKey1(), -1, null));
        }
        return this;
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

    //TODO : not sure if this is correct
    private boolean allFilesHaveBeenRead() {
        for (boolean b : this.allFilesHaveBeenRead) {
            if (!b)
                return false;
        }
        return true;
    }


    private Behavior<Message> handle(BatchMessage message) {
        this.getContext().getLog().info("Received batch of {} rows for file {}!", message.getBatch().size(), this.inputFiles[message.getId()].getName());
        List<String[]> batch = message.getBatch();
        if (!batch.isEmpty()) {
            int amountOfColumns = message.batch.get(0).length;
            for (int column = 0; column < amountOfColumns; column++) {
                for (String[] row : batch) {
                    if (row[column].matches("\\d+(-\\d+)*")) {
                        placingInBucket(message, column, row, keyMaker(this.inputFiles[message.getId()].getName(), this.headerLines[message.id][column]), false);
                    } else {
                        placingInBucket(message, column, row, keyMaker(this.inputFiles[message.getId()].getName(), this.headerLines[message.id][column]), true);
                    }
                }
            }
            // here we are telling the inputReader to read the next batch
            this.inputReaders.get(message.getId()).tell(new InputReader.ReadBatchMessage(this.getContext().getSelf()));
        } else {
            this.getContext().getLog().info("Finished reading file {}!", this.inputFiles[message.getId()].getName());
            this.allFilesHaveBeenRead[message.getId()] = true;
            if (allFilesHaveBeenRead()) {

                this.getContext().getLog().info("Size of columnOfNumbers is {} and of columOfStrings {}", columnOfNumbers.size(), columnOfStrings.size());
                this.getContext().getLog().info("Finished reading all files! Mining will start!");
                // Here we are telling the inputReader to read the next batch
                startMining();
            }
        }
        return this;
    }

    private int keyMaker(String tableName, String columnName) {
        String key = tableName + columnName;
        if (keyDictionary.containsKey(key))
            return keyDictionary.get(key);
        else {
            keyDictionary.put(key, key.hashCode());
            return key.hashCode();
        }
    }

    private void placingInBucket(BatchMessage message, int column, String[] row, int key, boolean isString) {
        if (isString) {
            if (columnOfStrings.containsKey(key))
                columnOfStrings.get(key).addValueToColumn(row[column]);
            else
                columnOfStrings.put(key, new Column(column, isString, this.headerLines[message.id][column], this.inputFiles[message.getId()].getName()));

        } else {
            if (columnOfNumbers.containsKey(key))
                columnOfNumbers.get(key).addValueToColumn(row[column]);
            else
                columnOfNumbers.put(key, new Column(column, isString, this.headerLines[message.id][column], this.inputFiles[message.getId()].getName()));
        }
    }

    private void startMining() {
        this.getContext().getLog().info("Starting mining!");
        creatingTaskLists();
        this.getContext().getLog().info("All task are now ready to be worked on {} tasks :)", this.listOfTasks.size());
        allTasksHavebeenCreated = true;
        // Giving dependencyWorker work
        for (ActorRef<DependencyWorker.Message> dependencyWorker : this.dependencyWorkers) {
            giveTasksToWorkers(dependencyWorker, -1);
        }
    }

    private void creatingTaskLists() {
        //This is for columnOfNumbers
        for (int key1 : columnOfNumbers.keySet()) {
            for (int key2 : columnOfNumbers.keySet()) {
                if (key1 != key2) {
                    taskDone.add(false);
                    listOfTasks.add(
                            new DependencyWorker.TaskMessage(
                                    null, -1,
                                    key1, key2,
                                    false));
                }
            }
        }
        //This is for columnOfStrings
        for (int key1 : columnOfStrings.keySet()) {
            for (int key2 : columnOfStrings.keySet()) {
                if (key1 != key2) {
                    taskDone.add(false);
                    listOfTasks.add(
                            new DependencyWorker.TaskMessage(
                                    null, -1,
                                    key1, key2,
                                    true));
                }

            }
        }
    }


    private Behavior<Message> handle(RegistrationMessage message) {
        ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
        if (!this.dependencyWorkers.contains(dependencyWorker)) {
            this.dependencyWorkers.add(dependencyWorker);
            this.dependencyWorkersLargeMessageProxy.add(message.getDependencyMinerLargeMessageProxy());
            this.getContext().watch(dependencyWorker);

            if (allTasksHavebeenCreated) {
                giveTasksToWorkers(dependencyWorker, -1);
            }
        }
        return this;
    }

    private boolean everyOneFinished() {
        //Works by the fact that when the list is empty, it means that all the tasks have been done
        for (Boolean b : taskDone) {
            if (!b)
                return false;
        }
        return true;
    }

    private void giveTasksToWorkers(ActorRef<DependencyWorker.Message> dependencyWorker, int taskId) {
        if (currentTask < listOfTasks.size()) {
            this.getContext().getLog().info("Giving task {} to worker {}", currentTask, dependencyWorker);
            DependencyWorker.TaskMessage task = this.listOfTasks.get(currentTask);
            task.setTaskId(currentTask);
            task.setDependencyMinerLargeMessageProxy(largeMessageProxy);
            this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(task, this.dependencyWorkersLargeMessageProxy.get(this.dependencyWorkers.indexOf(dependencyWorker))));
            currentTask++;
            this.lastWorker = dependencyWorker;
        } else {
            this.getContext().getLog().info("Finished giving all tasks to workers");

            if (everyOneFinished()) {
                this.getContext().getLog().info("All workers finished");
                this.end();
            }
        }
    }

    private Behavior<Message> handle(CompletionMessage message) {
        this.getContext().getLog().info("Received CompletionMessage from worker {} for task {}!", message.getDependencyWorker(), message.getTaskId());
        ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
        if (message.isHasDependency()) {
            this.getContext().getLog().info("Received IND from worker {} for task {}!", message.getDependencyWorker(), message.getTaskId());
            File dependentFile;
            File referencedFile;
            String[] dependentColumnName;
            String[] referencedColumnName;
            if (message.isString()) {
                dependentFile = new File(columnOfStrings.get(message.getKey1()).getNameOfDataset());
                referencedFile = new File(columnOfStrings.get(message.getKey2()).getNameOfDataset());
                dependentColumnName = new String[]{columnOfStrings.get(message.getKey1()).getColumnName()};
                referencedColumnName = new String[]{columnOfStrings.get(message.getKey1()).getColumnName()};
                this.getContext().getLog().info("IND is {} and {} from {} to {}!",
                        columnOfStrings.get(message.getKey1()).getColumnName(),
                        columnOfStrings.get(message.getKey2()).getNameOfDataset(),
                        columnOfStrings.get(message.getKey1()).getNameOfDataset(),
                        columnOfStrings.get(message.getKey2()).getNameOfDataset());

            } else {
                dependentFile = new File(columnOfNumbers.get(message.getKey1()).getNameOfDataset());
                referencedFile = new File(columnOfNumbers.get(message.getKey2()).getNameOfDataset());
                dependentColumnName = new String[]{columnOfNumbers.get(message.getKey1()).getColumnName()};
                referencedColumnName = new String[]{columnOfNumbers.get(message.getKey1()).getColumnName()};
                this.getContext().getLog().info("IND is {} and {} from {} to {}!",
                        columnOfNumbers.get(message.getKey1()).getColumnName(),
                        columnOfNumbers.get(message.getKey2()).getNameOfDataset(),
                        columnOfNumbers.get(message.getKey1()).getNameOfDataset(),
                        columnOfNumbers.get(message.getKey2()).getNameOfDataset());
            }


            InclusionDependency ind = new InclusionDependency(dependentFile, dependentColumnName, referencedFile, referencedColumnName);
            List<InclusionDependency> inds = new ArrayList<>();
            inds.add(ind);

            this.resultCollector.tell(new ResultCollector.ResultMessage(inds));
        }

        taskDone.remove(0);
        giveTasksToWorkers(dependencyWorker, message.getTaskId());
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
}