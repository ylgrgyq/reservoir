package com.github.ylgrgyq.reservoir.benchmark.storage;

import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.Spec;

@Command(name = "benchmark",
        synopsisSubcommandLabel = "COMMAND",
        version = "Reservoir benchmark tool v1.4-SNAPSHOT",
        description = "This is a dedicated benchmark testing tool for Reservoir. With this " +
                "tool you can challenge Reservoir under different working conditions like " +
                "different write size, different read size, different buffering mechanism, etc. Please " +
                "use it on your own machine to know if Reservoir meets your performance requirements.",
        commandListHeading = "%nCommands:%n%nThe most commonly used testing commands are:%n",
        mixinStandardHelpOptions = true,
        subcommands = {BenchmarkWriteTestMode.class})

public class BenchmarkOptions implements Runnable {
    @Spec
    private CommandSpec spec;

    @Override
    public void run() {
        throw new ParameterException(spec.commandLine(), "Missing required subcommand");
    }
}
