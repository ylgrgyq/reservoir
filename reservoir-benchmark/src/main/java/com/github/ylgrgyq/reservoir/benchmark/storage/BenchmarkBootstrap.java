package com.github.ylgrgyq.reservoir.benchmark.storage;

import org.apache.commons.cli.*;

public class BenchmarkBootstrap {
    public static void main(String[] args) throws Exception {
        BenchmarkOptions options = new BenchmarkOptions(args);

        final BenchmarkTest test;
        if (options.getStorageType().equals("RocksDB")) {
            test = new RocksDbStorageStoreBench(options.getDataSize(), options.getNumDataPerBatch(), options.getNumBatches());
        } else {
            test = new FileStorageStoreBench(options.getDataSize(), options.getNumDataPerBatch(), options.getNumBatches());
        }

        final BenchmarkRunner runner = new BenchmarkRunner();
        runner.runTest(test);
    }

    private static Options createCliOptions() {
        Options options = new Options();
        options.addOption(new Option("s", "data-size", true, "size in bytes for each data to store"));
        options.addOption(new Option("p", "data-per-batch", true, "number of data per batch"));
        options.addOption(new Option("n", "batches", true, "number of batches"));
        options.addOption(new Option("t", "storage-type", true, "storage type"));
        return options;
    }

    private static class BenchmarkOptions {
        private final int dataSize;
        private final int numDataPerBatch;
        private final int numBatches;
        private final String storageType;

        public BenchmarkOptions(String[] args) throws Exception {
            Options options = createCliOptions();
            CommandLineParser parser = new DefaultParser();

            CommandLine cmd = parser.parse(options, args);
            this.dataSize = Integer.parseInt(cmd.getOptionValue("data-size", "100"));
            this.numDataPerBatch = Integer.parseInt(cmd.getOptionValue("data-per-batch", "10"));
            this.numBatches = Integer.parseInt(cmd.getOptionValue("batches", "10000"));
            this.storageType = cmd.getOptionValue("storage-type", "file");
        }

        public int getDataSize() {
            return dataSize;
        }

        public int getNumDataPerBatch() {
            return numDataPerBatch;
        }

        public int getNumBatches() {
            return numBatches;
        }

        public String getStorageType() {
            return storageType;
        }
    }
}
