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

@Command(name = "read",
        showDefaultValues = true,
        sortOptions = false,
        headerHeading = "Usage:%n%n",
        optionListHeading = "%nOptions:%n",
        description = "All the tests in this command is only used to test the read performance " +
                "for Reservoir. During the test setup period, data for the read test will be written " +
                "to storage. After that, no more write operations will be issued. With the options of this " +
                "command, you can test Reservoir in different working conditions.",
        synopsisHeading = "%n",
        descriptionHeading = "%nDescription:%n%n",
        parameterListHeading = "%nParameters:%n",
        header = "Test the reading performance of Reservoir."
)
public class BenchmarkReadTestMode implements Callable<Integer> {
    @Spec
    private CommandSpec spec;

    @Option(names = {"-h", "--help"}, usageHelp = true, description = "Show this help message and exit.")
    private boolean usageHelpRequested;

    @Option(names = {"-s", "--data-size"},
            defaultValue = "100",
            description = "Size in bytes of each data.")
    private int dataSize;

    @Option(names = {"-p", "--number-of-data-per-read"},
            defaultValue = "10",
            description = "Number of data to retrieve in one read.")
    private int readBatchSize;

    @Option(names = {"-n", "--total-number-of-data-to-read"},
            defaultValue = "10000",
            description = "Total number of data to read.")
    private int numOfDataToRead;

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
            test = new RocksDbStorageReadBench(dataSize, readBatchSize, numOfDataToRead);
        } else {
            test = new FileStorageReadBench(dataSize, readBatchSize, numOfDataToRead, syncWriteWalLog);
        }

        final BenchmarkRunner runner = new BenchmarkRunner(runnerOptions);
        runner.runTest(test);

        return 0;
    }
}
