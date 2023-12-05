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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

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
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CompletionMessage implements Message {
        private static final long serialVersionUID = -7642425159675583598L;
        ActorRef<DependencyWorker.Message> dependencyWorker;
        int result;
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

        this.dependencyWorkers = new ArrayList<>();
        this.allFilesHaveBeenRead = new boolean[this.inputFiles.length];
        context.getSystem().receptionist().tell(Receptionist.register(dependencyMinerService, context.getSelf()));
    }

    /////////////////
    // Actor State //
    /////////////////

    private long startTime;

    private final boolean discoverNaryDependencies;
    private final File[] inputFiles;
    private final String[][] headerLines;
    private final boolean[] allFilesHaveBeenRead;
    // CompositeKey the first key is the name of the file and the second key is the name of the column
    private final HashMap<CompositeKey, Column> columnOfStrings = new HashMap<>();
    private final HashMap<CompositeKey, Column> columnOfNumbers = new HashMap<>();

    private final List<DependencyWorker.TaskMessage> listOfTasks = new ArrayList<>();

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

    //TODO : not sure if this is correct
    private boolean allFilesHaveBeenRead() {
        for (boolean b : this.allFilesHaveBeenRead) {
            if (!b) {
                return false;
            }
        }
        return true;
    }

    private Behavior<Message> handle(BatchMessage message) {
        this.getContext().getLog().info("Received batch of {} rows for file {}!", message.getBatch().size(), this.inputFiles[message.getId()].getName());
        List<String[]> batch = message.getBatch();
        if (!batch.isEmpty()) {
            int amountOfColumns = batch.get(0).length;
            for (int column = 0; column < amountOfColumns; column++) {
                for (String[] row : batch) {
                    if (row[column].matches("\\d+(-\\d+)*")) {
                        this.getContext().getLog().info("Adding new value to columnNumbers of {} from file {}!", this.headerLines[message.id][column], this.inputFiles[message.getId()].getName());
                        placingInBucket(message, column, row, columnOfNumbers);
                    } else {
                        this.getContext().getLog().info("Adding new value to columnStrings of {} from file {}!", this.headerLines[message.id][column], this.inputFiles[message.getId()].getName());
                        placingInBucket(message, column, row, columnOfStrings);
                    }
                }
                // here we are telling the inputReader to read the next batch
                this.inputReaders.get(message.getId()).tell(new InputReader.ReadBatchMessage(this.getContext().getSelf()));
            }
        } else {
            this.getContext().getLog().info("Finished reading file {}!", this.inputFiles[message.getId()].getName());
            this.allFilesHaveBeenRead[message.getId()] = true;
            if (allFilesHaveBeenRead()) {
                this.getContext().getLog().info("Finished reading all files! Mining will start!");
                // Here we are telling the inputReader to read the next batch
                startMining();
            }
        }
        return this;
    }

    private void placingInBucket(BatchMessage message, int column, String[] row, HashMap<CompositeKey, Column> columnOf) {
        if (columnOf.containsKey(new CompositeKey(this.inputFiles[message.getId()].getName(), this.headerLines[message.id][column])))
            columnOf.get((new CompositeKey(this.inputFiles[message.getId()].getName(), this.headerLines[message.id][column]))).addValueToColumn(row[column]);
        else {
            columnOf.put(new CompositeKey(this.inputFiles[message.getId()].getName(), this.headerLines[message.id][column]), new Column(column));
            columnOf.get(new CompositeKey(this.inputFiles[message.getId()].getName(), this.headerLines[message.id][column])).addValueToColumn(row[column]);
        }
    }

    private void startMining() {
            this.getContext().getLog().info("Starting mining!");

    }

    private void creatingTaskLists() {
        for (int i = 0; i < this.headerLines.length; i++) {
            for (int j = 0; j < this.headerLines[i].length; j++) {
                listOfTasks.add( new DependencyWorker.TaskMessage(
                        null,
                        -1,
                        i,
                        j,
                        i + 1,
                        j + 1)
                );
            }
        }
    }




    private Behavior<Message> handle(RegistrationMessage message) {
        ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
        if (!this.dependencyWorkers.contains(dependencyWorker)) {
            this.dependencyWorkers.add(dependencyWorker);

            // The worker should get some work ... let me send her something before I figure out what I actually want from her.
            // I probably need to idle the worker for a while if I do not have work for it right now ... (see a master/worker pattern)

        }
        return this;
    }

    private Behavior<Message> handle(CompletionMessage message) {
        ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
        // If this was a reasonable result, I would probably do something with it and potentially generate more work ... for now, let's just generate a random, binary IND.

        if (this.headerLines[0] != null) {
            Random random = new Random();
            int dependent = random.nextInt(this.inputFiles.length);
            int referenced = random.nextInt(this.inputFiles.length);
            File dependentFile = this.inputFiles[dependent];
            File referencedFile = this.inputFiles[referenced];
            String[] dependentAttributes = {this.headerLines[dependent][random.nextInt(this.headerLines[dependent].length)], this.headerLines[dependent][random.nextInt(this.headerLines[dependent].length)]};
            String[] referencedAttributes = {this.headerLines[referenced][random.nextInt(this.headerLines[referenced].length)], this.headerLines[referenced][random.nextInt(this.headerLines[referenced].length)]};
            InclusionDependency ind = new InclusionDependency(dependentFile, dependentAttributes, referencedFile, referencedAttributes);
            List<InclusionDependency> inds = new ArrayList<>(1);
            inds.add(ind);

            this.resultCollector.tell(new ResultCollector.ResultMessage(inds));
        }
        // I still don't know what task the worker could help me to solve ... but let me keep her busy.
        // Once I found all unary INDs, I could check if this.discoverNaryDependencies is set to true and try to detect n-ary INDs as well!


        // At some point, I am done with the discovery. That is when I should call my end method. Because I do not work on a completable task yet, I simply call it after some time.
        if (System.currentTimeMillis() - this.startTime > 2000000)
            this.end();
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