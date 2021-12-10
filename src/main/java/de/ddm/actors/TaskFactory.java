package de.ddm.actors;

import akka.actor.typed.ActorRef;
import de.ddm.actors.profiling.DependencyMiner;
import de.ddm.actors.profiling.DependencyWorker;

import java.util.*;

public class TaskFactory {
    private final ActorRef<DependencyMiner.Message> dependencyMinerRef;

    class TaskCounter implements Iterator<DependencyWorker.TaskMessage> {
        int referencedFileId;
        int nextDependentColumnIndex;

        Queue<Integer> remainingDependentFileIds = new LinkedList<>();

        public TaskCounter(int referencedFileId) {
            this.referencedFileId = referencedFileId;
            this.nextDependentColumnIndex = 0;
        }

        public void addDependentFile(int dependentFileId) {
            if(this.referencedFileId != dependentFileId) {
                this.remainingDependentFileIds.offer(dependentFileId);
            }
        }

        @Override
        public boolean hasNext() {
            return !this.remainingDependentFileIds.isEmpty();
        }

        @Override
        public DependencyWorker.TaskMessage next() {
            DependencyWorker.TaskMessage nextTaskMessage;

            int currentDependentFileId = this.remainingDependentFileIds.peek();

            nextTaskMessage = new DependencyWorker.TaskMessage(dependencyMinerRef, this.referencedFileId,
                    currentDependentFileId, this.nextDependentColumnIndex);
            this.nextDependentColumnIndex += 1;
            if (this.nextDependentColumnIndex >= fileToColumnCountMap.get(currentDependentFileId)) {
                this.nextDependentColumnIndex = 0;
                this.remainingDependentFileIds.poll();
            }

            return nextTaskMessage;
        }
    }

    Map<Integer, Integer> fileToColumnCountMap = new HashMap<>();
    Map<Integer, TaskCounter> fileToTaskCounter = new HashMap<>();

    Queue<Integer> nextReferencedFileId = new LinkedList<>();

    public TaskFactory(ActorRef<DependencyMiner.Message> dependencyMinerRef) {
        this.dependencyMinerRef = dependencyMinerRef;
    }

    public void addFile(int fileId, int columns) {
        // add new file id to all existing task counters
        for(TaskCounter tc : this.fileToTaskCounter.values()) {
            tc.addDependentFile(fileId);
        }
        // create new task counter for new file and add all existing file ids
        TaskCounter taskCounter = new TaskCounter(fileId);
        for(int dependentFileId : this.fileToTaskCounter.keySet()) {
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
}
