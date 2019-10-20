package com.github.ylgrgyq.reservoir.benchmark;


import picocli.CommandLine;

public class BenchmarkBootstrap {
    public static void main(String[] args) {
        CommandLine cli = new CommandLine(new BenchmarkOptions());
        int exitCode = cli.execute(args);
        System.exit(exitCode);
    }
}
