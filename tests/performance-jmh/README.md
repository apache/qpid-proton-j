Proton-J JMH Benchmarks
-------
This module contains optional [JMH](http://openjdk.java.net/projects/code-tools/jmh/) performance tests designed
to stress performance critical parts of the proton engine (eg encoders, decoders).

Note that this module is an optional part of the overall project build and does not deploy anything, due to its use
of JMH which is not permissively licensed. The module must either be built directly, or enabled
within the overall build by using the 'performance-jmh' maven profile.

Building the benchmarks
-------
The benchmarks are maven built and involve some code generation for the JMH part. As such it is required that you
rebuild upon changing the code. As the codebase is small it is recommended that you do this from the project
root folder to avoid missing any changes from other modules.

As noted above this module is optional in the main build, enabled by the performance-jmh profile, so to enable it
a command such as the following can be used from the root folder:

    mvn clean install -Pperformance-jmh

Running the benchmarks: General
-------
It is recommended that you consider some basic benchmarking practices before running benchmarks:

 1. Use a quiet machine with enough CPUs to run the number of threads you mean to run.
 2. Set the CPU freq to avoid variance due to turbo boost/heating.
 3. Use an OS tool such as taskset to pin the threads in the topology you mean to measure.

Running the JMH Benchmarks
-----
To run all JMH benchmarks:

    java -jar target/proton-j-performance-jmh.jar -f <number-of-forks> -wi <number-of-warmup-iterations> -i <number-of-iterations>
To list available benchmarks:

    java -jar target/proton-j-performance-jmh.jar -l
Some JMH help:

    java -jar target/proton-j-performance-jmh.jar -h

Example
-----
To run a benchmark on the String decoding while saving the results in json format:

    java -jar target/proton-j-performance-jmh.jar StringsBenchmark.decode* -f 1 -wi 5 -i 5 -rf json -rff strings_decode_before.json -gc true

After changing something in the String decode path and building the whole project again,
another snapshot of the current state of performance for the same case can be taken:

    java -jar target/proton-j-performance-jmh.jar StringsBenchmark.decode* -f 1 -wi 5 -i 5 -rf json -rff strings_decode_after.json -gc true

then it is possible to use many graphical tools to compare the results: one is [JMH Visualizer](http://jmh.morethan.io/). 
