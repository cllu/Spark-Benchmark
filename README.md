# Apache Spark vs Java 8 Stream

I am curious about what Apache Spark can offer compared to Java 8 parallel stream, on a local mode.

The task is to extract English labels for Wikidata entities. The Wikidata dump is 118G decompressed.

To run the Spark version (with output in `./data/labels-stream.txt`):

```bash
./gradlew runSpark -PsparkMain=me.cllu.benchmark.WikidataExtractorSpark -PsparkArgs="./data/wikidata-20170320-all.json local[4]"
```

To run the Java stream version (with output in `./data/labels-spark.txt/`):

```bash
./gradlew runMain -PmainClass=me.cllu.benchmark.WikidataExtractorStream -Parguments="./data/wikidata-20170320-all.json"
```

## Some results

With wikidata-20170320-all.json dump,
on my desktop PC with 4-cores, the Spark version (running locally) takes 14 minutes, while the Java stream version takes 24 minutes.
On a workstation with 16 cores, the Spark version takes 8 minutes, which the Java stream version takes 14 minutes.
