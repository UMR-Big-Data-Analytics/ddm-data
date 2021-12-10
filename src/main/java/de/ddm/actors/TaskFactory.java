package de.ddm.actors;

import akka.actor.typed.ActorRef;
import de.ddm.actors.profiling.DependencyMiner;
import de.ddm.actors.profiling.DependencyWorker;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

public class TaskFactory {
    private final ActorRef<DependencyMiner.Message> dependencyMinerRef;

    class TaskCounter implements Iterator<DependencyWorker.TaskMessage> {
        int referencedFileId;
        int nextDependentColumnIndex;
        int currentDependentFileId;

        public TaskCounter(int referencedFileId) {
            this.referencedFileId = referencedFileId;
            this.nextDependentColumnIndex = 0;
            this.currentDependentFileId = referencedFileId == 0 ? 1 : 0;
        }

        @Override
        public boolean hasNext() {
            return currentDependentFileId < numberOfFiles;
        }

        @Override
        public DependencyWorker.TaskMessage next() {
            DependencyWorker.TaskMessage nextTaskMessage;

            nextTaskMessage = new DependencyWorker.TaskMessage(dependencyMinerRef, this.referencedFileId,
                    this.currentDependentFileId, this.nextDependentColumnIndex);
            this.nextDependentColumnIndex += 1;
            if (this.nextDependentColumnIndex >= columnNumberOfFiles[this.currentDependentFileId]) {
                this.nextDependentColumnIndex = 0;
                this.currentDependentFileId += 1;
                if (this.referencedFileId == this.currentDependentFileId) {
                    this.currentDependentFileId += 1;
                }
            }

            return nextTaskMessage;
        }
    }

    int numberOfFiles;
    int[] columnNumberOfFiles;
    TaskCounter[] taskCounters;

    Queue<Integer> nextReferencedFileId = new LinkedList<>();

    public TaskFactory(ActorRef<DependencyMiner.Message> dependencyMinerRef, String[][] headerLines) {
        this.dependencyMinerRef = dependencyMinerRef;
        this.numberOfFiles = headerLines.length;
        this.columnNumberOfFiles = new int[headerLines.length];
        this.taskCounters = new TaskCounter[headerLines.length];
        for (int i = 0; i < this.columnNumberOfFiles.length; i++) {
            this.columnNumberOfFiles[i] = headerLines[i].length;
            this.taskCounters[i] = new TaskCounter(i);
            this.nextReferencedFileId.offer(i);
        }

    }

    public boolean hasNextTaskByReferencedFile(int referencedFileId) {
        return this.taskCounters[referencedFileId].hasNext();
    }

    public DependencyWorker.TaskMessage nextTaskByReferencedFile(int referencedFileId) {
        DependencyWorker.TaskMessage message = this.taskCounters[referencedFileId].next();
        if (!hasNextTaskByReferencedFile(referencedFileId)) {
            nextReferencedFileId.remove(referencedFileId);
        }
        return message;
    }

    public boolean isAllWorkDistributed() {
        return this.nextReferencedFileId.isEmpty();
    }

    public Integer nextReferencedFileId() {
        Integer nextId = this.nextReferencedFileId.poll();
        this.nextReferencedFileId.offer(nextId);
        return nextId;
    }
}
