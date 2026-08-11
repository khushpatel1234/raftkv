package io.github.khushpatel1234.raftkv.storage;

/** Immutable snapshot of successful group-commit activity. */
public record GroupCommitMetrics(
        long batchCount,
        long entryCount,
        int maxBatchSize,
        double averageBatchSize) {

    public GroupCommitMetrics {
        if (batchCount < 0 || entryCount < 0 || maxBatchSize < 0 || averageBatchSize < 0) {
            throw new IllegalArgumentException("Metrics cannot be negative");
        }
    }

    public long totalEntries() {
        return entryCount;
    }

    public int maxBatchEntries() {
        return maxBatchSize;
    }

    public double averageBatchEntries() {
        return averageBatchSize;
    }
}
