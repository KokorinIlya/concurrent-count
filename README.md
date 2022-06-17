# concurrent-count

## Testing
* `./gradlew test`

## Building benchmarks
* `./gradlew jmhJar`

## Benchmarking
* `java -jar build/libs/jvm-count-jmh.jar <bench_name> -t <number of threads> -p <key 1>=<value 1> <key 2>=<value 2> ...`
* Example: `java -jar build/libs/jvm-count-jmh.jar bench.set.SuccessfulInsertBenchmark -t 16 -p size=1000000`
* Run `java -jar build/libs/jvm-count-jmh.jar -h` for different run-time options

## Useful options
* General options:
  * `-bm <mode>` to specify benchmark mode (`[Throughput/thrpt, AverageTime/avgt, SampleTime/sample, SingleShotTime/ss, All/all]`).
  * `-t <int>` to specify the number of threads.
* Measurement options:
  * `-f <int>` to specify the number of forks, the benckmark should perform.
  * `-i <int>` to specify the number of benchmark iterations.
  * `-r <time>` to specify the time, spent at each measurement iteration. Time should be specified in `10 s` format.
* Warmup options:
  * `-wf <int>` to specify the number of warmup forks.
  * `-wi <int>` to specify the number of warmup iterations.
  * `-w <time>` to specify time, spent at each warmup iteration.
