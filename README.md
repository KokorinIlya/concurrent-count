# concurrent-count

## Testing
* `./gradlew test`

## Building benchmarks
* `./gradlew jmhJar`

## Benchmarking
* `java -jar build/libs/jvm-count-jmh.jar <bench_name> -t <number of threads> -p <key 1>=<value 1> <key 2>=<value 2> ...`
* Example: `java -jar build/libs/jvm-count-jmh.jar bench.set.SuccessfulInsertBenchmark -t 16 -p size=1000000`
