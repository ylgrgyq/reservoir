package com.github.ylgrgyq.reservoir.benchmark.storage;

import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

import java.util.concurrent.Callable;

@Command(name = "write",
        showDefaultValues = true,
        sortOptions = false,
        headerHeading = "Usage:%n%n",
        optionListHeading = "%nOptions:%n",
        description = "Test write performance.",
        synopsisHeading = "%n",
        descriptionHeading = "%nDescription:%n%n",
        parameterListHeading = "%nParameters:%n",
        header = "Record changes to the repository."
)
public class BenchmarkWriteTestMode implements Callable<Integer> {
    @Spec
    private CommandSpec spec;

    @Option(names = {"-h", "--help"}, usageHelp = true, description = "Show this help message and exit.")
    private boolean usageHelpRequested;

    @Option(names = {"-s", "--data-size"},
            defaultValue = "100",
            description = "Size in bytes for each data to store.")
    private int dataSize;

    @Option(names = {"-p", "--data-per-batch"},
            defaultValue = "10",
            description = "Number of data per batch.")
    private int dataPerBatch;

    @Option(names = {"-n", "--batches"},
            defaultValue = "10000",
            description = "Number of batches.")
    private int batches;

    @Option(names = {"-t", "--storage-type"},
            defaultValue = "FileStorage",
            description = "Storage type, valid values: ${COMPLETION-CANDIDATES}.")
    private StorageType storageType;

    @Override
    public Integer call() throws Exception {
        if (usageHelpRequested) {
            spec.commandLine().usage(System.out);
            return 0;
        }

        final BenchmarkTest test;
        if (storageType == StorageType.RocksDBStorage) {
            test = new RocksDbStorageStoreBench(dataSize, dataPerBatch, batches);
        } else {
            test = new FileStorageStoreBench(dataSize, dataPerBatch, batches);
        }

        final BenchmarkRunner runner = new BenchmarkRunner();
        runner.runTest(test);

        return 0;
    }

    private enum StorageType {
        RocksDBStorage,
        FileStorage
    }
}
