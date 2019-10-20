package com.github.ylgrgyq.reservoir.benchmark.storage;

import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.Spec;

import java.util.concurrent.Callable;

@Command(name = "storage",
        synopsisSubcommandLabel = "COMMAND",
        description = "Test the performance of the underlying storage.",
        commandListHeading = "%nCommands:%n%nThe testing commands are:%n",
        subcommands = {BenchmarkWriteTestMode.class, BenchmarkReadTestMode.class})
public class StorageBenchmarkOptions implements Callable<Integer> {
    @Spec
    private CommandSpec spec;

    @Option(names = {"-h", "--help"}, usageHelp = true, description = "Show this help message and exit.")
    private boolean usageHelpRequested;

    @Override
    public Integer call() {
        if (usageHelpRequested) {
            spec.commandLine().usage(System.out);
            return 0;
        }

        throw new ParameterException(spec.commandLine(), "Missing required subcommand");
    }
}
