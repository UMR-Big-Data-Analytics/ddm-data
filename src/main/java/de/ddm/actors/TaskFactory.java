package de.ddm.actors;

import akka.actor.typed.ActorRef;
import de.ddm.actors.profiling.DependencyMiner;
import de.ddm.actors.profiling.DependencyWorker;
import de.ddm.structures.InclusionDependency;

import java.util.*;

public class TaskFactory {
    public interface InclusionDependencyMapper {
        InclusionDependency map(ColumnId referencedColumnId, ColumnId dependentColumnId);
    }

    private final String[][][] contents;
    Map<TaskId, Integer> taskTrackerMap = new HashMap<>();

    public InclusionDependency handleCompletionMessage(DependencyMiner.CompletionMessage completionMessage,
                                                             InclusionDependencyMapper mapper) {

        TaskId taskId = new TaskId(completionMessage.getReferencedColumnId(),
                completionMessage.getDependentColumnId());

        if (completionMessage.isCandidate()) {
            Integer integer = taskTrackerMap.remove(taskId);
            if (integer != null) {
                int newValue = integer - 1;
                if (newValue == 0) {
                    return mapper.map(taskId.referencedColumn, taskId.dependentColumn);
                    // we got an inclusion dependency
                } else {
                    taskTrackerMap.put(taskId, newValue);
                }
            }
        } else {
            taskTrackerMap.remove(taskId);
        }

        return null;
    }

    class TaskCounter implements Iterator<DependencyWorker.TaskMessage> {
        ColumnId referencedColumnId;
        int nextBatchStartIndex;

        final int BATCH_SIZE = 30000;

        Queue<DependencyWorker.TaskMessage> failedTasks = new LinkedList<>();
        Queue<ColumnId> remainingDependentColumnIds = new LinkedList<>();

        DependencyWorker.TaskMessage nextMessage;

        public TaskCounter(ColumnId referencedColumnId) {
            this.referencedColumnId = referencedColumnId;
            this.nextBatchStartIndex = 0;
            this.nextMessage = computeNext();
        }

        public void addDependentColumn(ColumnId dependendColumnId) {
            if (this.referencedColumnId.isDifferentFile(dependendColumnId)) {
                this.remainingDependentColumnIds.offer(dependendColumnId);
            }
        }

        @Override
        public boolean hasNext() {
            if (nextMessage == null) {
                nextMessage = computeNext();
            }
            return nextMessage != null;
        }

        @Override
        public DependencyWorker.TaskMessage next() {
            DependencyWorker.TaskMessage result = this.nextMessage;
            this.nextMessage = this.computeNext();
            return result;
        }

        private DependencyWorker.TaskMessage computeNext() {
            if (!failedTasks.isEmpty()) {
                return failedTasks.poll();
            }

            if (this.remainingDependentColumnIds.isEmpty()) {
                return null;
            }

            DependencyWorker.TaskMessage nextTaskMessage;

            ColumnId currentDependentColumnId = this.remainingDependentColumnIds.peek();

            String[] dependentColumn = contents[currentDependentColumnId.getFileId()][currentDependentColumnId.getColumnId()];
            int dependentColumnSize = dependentColumn.length;

            boolean isCandidate = false;

            int batchCount = (int) Math.ceil((double) dependentColumnSize / BATCH_SIZE);
            TaskId taskId = new TaskId(this.referencedColumnId, currentDependentColumnId);
            if (nextBatchStartIndex == 0) {
                taskTrackerMap.put(taskId, batchCount);
            }
            if (taskTrackerMap.containsKey(taskId)) {
                isCandidate = true;
            }

            int from = nextBatchStartIndex;
            int to = Math.min(nextBatchStartIndex + BATCH_SIZE, dependentColumnSize);

            nextTaskMessage = new DependencyWorker.TaskMessage(dependencyMinerRef, this.referencedColumnId,
                    currentDependentColumnId, from, to);

            nextBatchStartIndex = to;

            if (!isCandidate || nextBatchStartIndex >= dependentColumnSize) {
                nextBatchStartIndex = 0;
                this.remainingDependentColumnIds.poll(); // remove the column id
            }

            if (!isCandidate) {
                nextTaskMessage = this.computeNext();
            }

            return nextTaskMessage;
        }
    }

    private final ActorRef<DependencyMiner.Message> dependencyMinerRef;

    Map<ColumnId, TaskCounter> columnToTaskCounter = new HashMap<>();

    Queue<ColumnId> nextReferencedColumnId = new LinkedList<>();

    public TaskFactory(ActorRef<DependencyMiner.Message> dependencyMinerRef, String[][][] contents) {
        this.dependencyMinerRef = dependencyMinerRef;
        this.contents = contents;
    }

    public void addFile(int fileId, int columns) {
        for(int i=0; i < columns; i++) {
            ColumnId columnId = new ColumnId(fileId, i);
            // add new file id to all existing task counters
            for (TaskCounter tc : this.columnToTaskCounter.values()) {
                tc.addDependentColumn(columnId);
            }
            // create new task counter for new file and add all existing file ids
            TaskCounter taskCounter = new TaskCounter(columnId);
            for (ColumnId dependentColumnId : this.columnToTaskCounter.keySet()) {
                taskCounter.addDependentColumn(dependentColumnId);
            }
            this.columnToTaskCounter.put(columnId, taskCounter);
            // reset referencedFileId queue when there is a new file
            this.nextReferencedColumnId.clear();
            this.nextReferencedColumnId.addAll(this.columnToTaskCounter.keySet());
        }
    }

    public boolean hasNextTaskByReferencedColumn(ColumnId columnId) {
        return this.columnToTaskCounter.get(columnId).hasNext();
    }

    public DependencyWorker.TaskMessage nextTaskByReferencedFile(ColumnId columnId) {
        DependencyWorker.TaskMessage message = this.columnToTaskCounter.get(columnId).next();
        if (!hasNextTaskByReferencedColumn(columnId)) {
            nextReferencedColumnId.remove(columnId);
        }
        return message;
    }

    public boolean hasWork() {
        return this.nextReferencedColumnId.stream().anyMatch((id) -> this.columnToTaskCounter.get(id).hasNext());
    }

    public ColumnId nextReferencedColumnId() {
        ColumnId nextId = this.nextReferencedColumnId.poll();
        this.nextReferencedColumnId.offer(nextId);
        return nextId;
    }

    public void addFailedTask(DependencyWorker.TaskMessage taskMessage) {
        if (!this.nextReferencedColumnId.contains(taskMessage.getReferencedColumnId())) {
            this.nextReferencedColumnId.offer(taskMessage.getReferencedColumnId());
        }
        this.columnToTaskCounter.get(taskMessage.getReferencedColumnId()).failedTasks.offer(taskMessage);
    }
}
