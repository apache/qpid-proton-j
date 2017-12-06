Proton-J Benchmarks
-------
This project contains performance tests designed to stress performance critical parts of the proton engine 
(eg encoders, decoders).

Building the benchmarks
-------
The benchmarks are maven built and involve some code generation for the JMH part. As such it is required that you
run 'mvn clean install' on changing the code. As the codebase is rather small it is recommended that you run this
command from the parent folder to avoid missed changes from other packages.

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
    
After changed something in the String decode path and compiled the whole project, can be taken 
another snapshot of the current state of performances for the same case:

    java -jar target/proton-j-performance-jmh.jar StringsBenchmark.decode* -f 1 -wi 5 -i 5 -rf json -rff strings_decode_after.json -gc true   

then it is possible to use many graphical tools to compare the results: one is [JMH Visualizer](http://jmh.morethan.io/). 
