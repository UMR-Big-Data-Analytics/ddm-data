package de.ddm.actors;

import akka.actor.typed.ActorRef;
import de.ddm.actors.profiling.DependencyMiner;
import de.ddm.actors.profiling.DependencyWorker;
import de.ddm.structures.InclusionDependency;

import java.util.*;

public class TaskFactory {
    public interface InclusionDependencyMapper {
        InclusionDependency map(int referencedFileId, int referencedColumnId, int dependentFileId,
                                int dependentColumnId);
    }

    private final String[][][] contents;
    Map<TaskId, Integer> taskTrackerMap = new HashMap<>();

    public List<InclusionDependency> handleCompletionMessage(DependencyMiner.CompletionMessage completionMessage,
                                                             InclusionDependencyMapper mapper) {
        List<InclusionDependency> inclusionDependencies = new ArrayList<>();
        for (int i = 0; i < contents[completionMessage.getReferencedFileId()].length; i++) {

            TaskId taskId = new TaskId(completionMessage.getReferencedFileId(),
                    completionMessage.getMaybeDependentFileId(), i,
                    completionMessage.getMaybeDependentColumnId());

            if (completionMessage.getReferencedColumnCandidates().contains(i)) {
                Integer integer = taskTrackerMap.get(taskId);
                if (integer != null) {
                    int newValue = integer - 1;
                    if (newValue == 0) {
                        inclusionDependencies.add(mapper.map(taskId.referencedFileId, taskId.referencedFileColumnId,
                                taskId.dependentFileId, taskId.dependentFileColumnId));
                        // we got an inclusion dependency
                    } else {
                        taskTrackerMap.put(taskId, newValue);
                    }

                }
            } else {
                taskTrackerMap.remove(taskId);
            }
        }

        return inclusionDependencies;
    }

    class TaskCounter implements Iterator<DependencyWorker.TaskMessage> {
        int referencedFileId;
        int nextDependentColumnIndex;
        int nextBatchStartIndex;

        final int BATCH_SIZE = 10000;

        Queue<DependencyWorker.TaskMessage> failedTasks = new LinkedList<>();
        Queue<Integer> remainingDependentFileIds = new LinkedList<>();

        public TaskCounter(int referencedFileId) {
            this.referencedFileId = referencedFileId;
            this.nextDependentColumnIndex = 0;
            this.nextBatchStartIndex = 0;
        }

        public void addDependentFile(int dependentFileId) {
            if (this.referencedFileId != dependentFileId) {
                this.remainingDependentFileIds.offer(dependentFileId);
            }
        }

        @Override
        public boolean hasNext() {
            return !this.remainingDependentFileIds.isEmpty() || !this.failedTasks.isEmpty();
        }

        @Override
        public DependencyWorker.TaskMessage next() {
            if (!failedTasks.isEmpty()) {
                return failedTasks.poll();
            }

            DependencyWorker.TaskMessage nextTaskMessage;

            int currentDependentFileId = this.remainingDependentFileIds.peek();

            String[] dependentColumn = contents[currentDependentFileId][this.nextDependentColumnIndex];
            int dependentColumnSize = dependentColumn.length;

            if (nextBatchStartIndex == 0) {
                int batchCount = (int) Math.ceil((double) dependentColumnSize / BATCH_SIZE);
                for (int i = 0; i < contents[this.referencedFileId].length; i++) {
                    TaskId taskId = new TaskId(this.referencedFileId, currentDependentFileId, i,
                            this.nextDependentColumnIndex);
                    taskTrackerMap.put(taskId, batchCount);
                }
            }

            // TODO potential for skipping useless work

            int from = nextBatchStartIndex;
            int to = Math.min(nextBatchStartIndex + BATCH_SIZE, dependentColumnSize);

            nextTaskMessage = new DependencyWorker.TaskMessage(dependencyMinerRef, this.referencedFileId,
                    currentDependentFileId, this.nextDependentColumnIndex, from, to);

            nextBatchStartIndex = to;

            if (nextBatchStartIndex >= dependentColumnSize) {
                this.nextDependentColumnIndex += 1;
                nextBatchStartIndex = 0;
                if (this.nextDependentColumnIndex >= fileToColumnCountMap.get(currentDependentFileId)) {
                    this.nextDependentColumnIndex = 0;
                    this.remainingDependentFileIds.poll(); // remove the file id
                }

            }

            return nextTaskMessage;
        }
    }

    private final ActorRef<DependencyMiner.Message> dependencyMinerRef;

    Map<Integer, Integer> fileToColumnCountMap = new HashMap<>();
    Map<Integer, TaskCounter> fileToTaskCounter = new HashMap<>();


    Queue<Integer> nextReferencedFileId = new LinkedList<>();

    public TaskFactory(ActorRef<DependencyMiner.Message> dependencyMinerRef, String[][][] contents) {
        this.dependencyMinerRef = dependencyMinerRef;
        this.contents = contents;
    }

    public void addFile(int fileId, int columns) {
        // add new file id to all existing task counters
        for (TaskCounter tc : this.fileToTaskCounter.values()) {
            tc.addDependentFile(fileId);
        }
        // create new task counter for new file and add all existing file ids
        TaskCounter taskCounter = new TaskCounter(fileId);
        for (int dependentFileId : this.fileToTaskCounter.keySet()) {
            taskCounter.addDependentFile(dependentFileId);
        }
        this.fileToColumnCountMap.put(fileId, columns);
        this.fileToTaskCounter.put(fileId, taskCounter);
        // reset referencedFileId queue when there is a new file
        this.nextReferencedFileId.clear();
        this.nextReferencedFileId.addAll(this.fileToTaskCounter.keySet());
    }

    public boolean hasNextTaskByReferencedFile(int referencedFileId) {
        return this.fileToTaskCounter.get(referencedFileId).hasNext();
    }

    public DependencyWorker.TaskMessage nextTaskByReferencedFile(int referencedFileId) {
        DependencyWorker.TaskMessage message = this.fileToTaskCounter.get(referencedFileId).next();
        if (!hasNextTaskByReferencedFile(referencedFileId)) {
            nextReferencedFileId.remove(referencedFileId);
        }
        return message;
    }

    public boolean hasWork() {
        return this.nextReferencedFileId.stream().anyMatch((id) -> this.fileToTaskCounter.get(id).hasNext());
    }

    public Integer nextReferencedFileId() {
        Integer nextId = this.nextReferencedFileId.poll();
        this.nextReferencedFileId.offer(nextId);
        return nextId;
    }

    public void addFailedTask(DependencyWorker.TaskMessage taskMessage) {
        if (!this.nextReferencedFileId.contains(taskMessage.getReferencedFileId())) {
            this.nextReferencedFileId.offer(taskMessage.getReferencedFileId());
        }
        this.fileToTaskCounter.get(taskMessage.getReferencedFileId()).failedTasks.offer(taskMessage);
    }
}
