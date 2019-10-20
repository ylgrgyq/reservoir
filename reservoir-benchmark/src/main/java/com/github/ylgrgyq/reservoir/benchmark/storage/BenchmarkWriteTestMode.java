package com.github.ylgrgyq.reservoir.benchmark.storage;

import com.github.ylgrgyq.reservoir.benchmark.BenchmarkRunner;
import com.github.ylgrgyq.reservoir.benchmark.BenchmarkRunnerOptions;
import com.github.ylgrgyq.reservoir.benchmark.BenchmarkTest;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

import java.util.concurrent.Callable;

@Command(name = "write",
        showDefaultValues = true,
        sortOptions = false,
        headerHeading = "Usage:%n%n",
        optionListHeading = "%nOptions:%n",
        description = "All the tests in this command is only used to test the write performance " +
                "for Reservoir. During the test, no read operations will be issued. With the options of this " +
                "command, you can test Reservoir in different working conditions.",
        synopsisHeading = "%n",
        descriptionHeading = "%nDescription:%n%n",
        parameterListHeading = "%nParameters:%n",
        header = "Test the writing performance of Reservoir."
)
public class BenchmarkWriteTestMode implements Callable<Integer> {
    @Spec
    private CommandSpec spec;

    @Option(names = {"-h", "--help"}, usageHelp = true, description = "Show this help message and exit.")
    private boolean usageHelpRequested;

    @Option(names = {"-s", "--data-size"},
            defaultValue = "100",
            description = "Size in bytes of each data to write to Reservoir.")
    private int dataSize;

    @Option(names = {"-p", "--number-of-data-per-batch"},
            defaultValue = "10",
            description = "Number of data per batch to write to Reservoir.")
    private int numberOfDataPerBatch;

    @Option(names = {"-n", "--number-of-batches"},
            defaultValue = "10000",
            description = "Number of batches of data to write to Reservoir for each tests.")
    private int numberOfBatches;

    @Mixin
    private BenchmarkRunnerOptions runnerOptions;

    @Option(names = {"--sync-write-wal"},
            defaultValue = "false",
            description = "Flush underlying WAL log in storage synchronously after every write.")
    private boolean syncWriteWalLog;

    @Option(names = {"-T", "--storage-type"},
            defaultValue = "FileStorage",
            description = "The underlying storage type used by this test. Valid values is: ${COMPLETION-CANDIDATES}.")
    private StorageType storageType;

    @Override
    public Integer call() throws Exception {
        if (usageHelpRequested) {
            spec.commandLine().usage(System.out);
            return 0;
        }

        final BenchmarkTest test;
        if (storageType == StorageType.RocksDBStorage) {
            test = new RocksDbStorageWriteBench(dataSize, numberOfDataPerBatch, numberOfBatches);
        } else {
            test = new FileStorageWriteBench(dataSize, numberOfDataPerBatch, numberOfBatches, syncWriteWalLog);
        }

        final BenchmarkRunner runner = new BenchmarkRunner(runnerOptions);
        runner.runTest(test);

        return 0;
    }
}
