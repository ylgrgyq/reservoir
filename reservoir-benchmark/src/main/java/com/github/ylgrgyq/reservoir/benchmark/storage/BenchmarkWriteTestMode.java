package com.github.ylgrgyq.reservoir.benchmark.storage;

import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

import java.util.concurrent.Callable;

@Command(name = "write",
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
    CommandSpec spec;

    @Option(names = {"-s", "--data-size"}, description = "Size in bytes for each data to store")
    int dataSize;

    @Option(names = {"-p", "--data-per-batch"}, description = "Number of data per batch")
    int dataPerBatch;

    @Option(names = {"-n", "--batches"}, description = "Number of batches")
    int batches;

    @Option(names = {"-t", "--storage-type"}, description = "Storage type")
    String storageType;

    @Override
    public Integer call() throws Exception {
        System.out.println("data size:" + dataSize + " data per batch:" + dataPerBatch +
                " batches=" + batches + " storage type:" + storageType);
        spec.commandLine().usage(System.out);
        return 0;
    }
}
