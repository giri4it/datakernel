---
id: streams
filename: streams/
title: DataStreams Module
prev: modules/http.html
next: modules/serializer.html
---

DataStreams Module is useful for intra- and inter-server communication and asynchronous data processing.

It is mostly a bare building block for other DataKernel modules (OLAP Cube, Aggregation, RPC and so on)

DataStreams is:
* Modern implementation of async reactive streams (unlike streams in Java 8 and traditional thread-based blocking streams)
* Asynchronous with extremely efficient congestion control, to handle natural imbalance in speed of data sources
* Composable stream operations (mappers, reducers, filters, sorters, mergers/splitters, compression, serialization)
* Stream-based network and file I/O on top of eventloop module

## Examples

Very simple implementation (<100 lines of code!) of interserver stream:
1. [Network Demo](https://github.com/softindex/datakernel-examples/blob/master/examples/datastreams/src/main/java/io/datakernel/examples/NetworkDemoServer.java)

It's stream graph is illustrated in the picture below:

<img src="http://www.plantuml.com/plantuml/png/dPH1RiCW44Ntd694Dl72aT83LBb3J-3QqmJLPYmO9qghtBrGspME0uwwPHwVp_-2W-N2SDVKmZAPueWWtz2SqS1cB-5R0A1cnLUGhQ6gAn6KPYk3TOj65RNwGk0JDdvCy7vbl8DqrQy2UN67WaQ-aFaCCOCbghDN8ei3_s6eYV4LJgVtzE_nbetInvc1akeQInwK1y3HK42jB4jnMmRmCWzWDFTlM_V9bTIq7Kzk1ablqADWgS4JNHw7FLqXcdUOuZBrcn3RiDCCylmLjj4wCv6OZNkZBMT29CUmspc1TCHUOuNeVIJoTxT8JVlzJnRZj9ub8U_QURhB_cO1FnXF6YlT_cMTXEQ9frvSc7kI6nscdsMyWX4OTLOURIOExfRkx_e1">

Note that this example is still very simple, as big graphs could span over numerous servers and process a lot of data in various ways.

Also there are some examples of creating own stream node:

1. [Simple Producer](https://github.com/softindex/datakernel-examples/blob/master/examples/datastreams/src/main/java/io/datakernel/examples/ProducerExample.java)
2. [Simple Consumer](https://github.com/softindex/datakernel-examples/blob/master/examples/datastreams/src/main/java/io/datakernel/examples/ConsumerExample.java)
3. [Custom Transformer](https://github.com/softindex/datakernel-examples/blob/master/examples/datastreams/src/main/java/io/datakernel/examples/TransformerExample.java)


To run the examples, you should execute these lines in the console in appropriate folder:
{% highlight bash %}
$ git clone https://github.com/softindex/datakernel-examples.git
$ cd datakernel-examples/examples/datastreams
$ mvn clean package exec:java -Dexec.mainClass=io.datakernel.examples.NetworkDemoServer
$ # in another console
$ mvn clean package exec:java -Dexec.mainClass=io.datakernel.examples.NetworkDemoClient
{% endhighlight %}
 Or this lines:
{% highlight bash %}
$ $ mvn clean package exec:java -Dexec.mainClass=io.datakernel.examples.ProducerExample
$ # OR
$ mvn clean package exec:java -Dexec.mainClass=io.datakernel.examples.ConsumerExample
$ # OR
$ mvn clean package exec:java -Dexec.mainClass=io.datakernel.examples.TransformerExample
{% endhighlight %}

Note that for network demo you should first launch the server and then the client.

## Stream Primitives

There are dozens of builtin primitives which you just need to wire to each other (the example above from the picture proves it).

Here is a list of them with short descriptions:

* Producers:
  * StreamProducer.idle() - does not send any data nor closes itself.
  * StreamProducer.closing() - does not send any data but closes itself immediately after binding.
  * StreamProducer.closingWithError(Throwable) - closes with given error after binding.
  * StreamProducer.endOfStreamOnError() - a wrapper which closes itself whether given stream closes with error or not.
  * StreamProducer.noEndOfStream() - a wrapper which never closes.
  * StreamProducer.of(values...) - sends given values and then closes.
  * StreamProducer.ofIterator(iterator) - sends values from given iterator until it finishes and then closes.
  * StreamProducer.ofIterable(iterable) - same as above.
  * StreamProducer.ofStage(stage) - a wrapper which unwraps producer from a CompletionStage (starts sending data from producer from stage when stage is complete).
  * StreamProducer.ofStageWithResult(CompletionStage<StreamProducerWithResult>) - same as above but for producers that have a result.
  * StreamProducer.withEndOfStreamAsResult(StreamProducer) - wrapper which returns empty result when stream closes.
  * StreamProducer.withResult(StreamProducer, CompletionStage) - wrapper which assigns given CompletionStage as a result to given producer.
  * StreamProducer.concat(StreamProducer...) - wrapper which concatenates given producers.
  * StreamFileReader - producer which allows to read data from file non-blockingly.

* Consumers:
  * StreamConsumer.idle() - does nothing, when wired producer finishes, it sets consumer's status as finished too.
  * StreamConsumer.errorDecorator(StreamConsumer, Predicate, Supplier<Throwable>) - a wrapper which closes with given error when predicate fails on consumed item.
  * StreamConsumer.suspendDecorator(StreamConsumer, Predicate, ...) - a wrapper which suspends when predicate fails on consumed item.
  * StreamFileWriter - consumer which allows to non-blockingly write data to file.

* Transformers:
  * CountingStreamForwarder - does not transform data, but counts how many items passed through it.
  * StreamBinarySerializer/Deserializer - transforms data to/from ByteBuf's using given serializer.
  * StreamByteChunker - transforms only ByteBuf's into ByteBuf's, slicing them in smaller chunks.
  * StreamFilter - passes through only those items which matched given predicate.
  * StreamForwarder - not really a transformer, does nothing to data, but wires together producer and consumer wrapped in CompletionStage's.
  * StreamFunction - converts given items into other using given function (equivalent of the map operation).
  * StreamJoin - complicated transfomer which joins more than one producer into one consumer with strategies and mapping functions.
  * StreamLZ4Compressor/Decompressor - transforms only ByteBuf's into ByteBuf's, compressing/decompressing them with LZ4 algorithm.
  * StreamMap - smarter version of StreamFunction which can transform one item into various number of other items (equivalent of the flatMap operation).
  * StreamMerger - Merges streams sorted by keys and streams their sorted union.
  * StreamReducer - Performs aggregative functions on the elements from input streams sorted by keys. Searches key of item with key function, selects elements with some key, reduces it and streams the results sorted by key.
  * StreamReducerSimple - Performs a reduction on the elements of input streams using the key function.
  * StreamSharder - Divides input stream into groups with some key function, and sends obtained streams to consumers.
  * StreamSorter - Receives data and saves it, and on end of stream sorts it and streams to the destination.
  * StreamSplitter - Sends received items into multiple consumers at once.
  * StreamUnion - Unions all input streams and streams their items in order of receiving them to the destination.

## Benchmark

We have measured the performance of our streams under various use scenarios.

Results are shown in the table below.

In every scenario producer generates 1 million numbers from 1 to 1,000,000.

Columns describe the different behaviour of the consumer (backpressure): whether it suspends and how often.

Numbers denote how many items has been processed by each stream graph per second (on a single core).

<table>
    <tr>
        <th rowspan="2">Use case</th>
        <th colspan="3">Consumer suspends</th>
    </tr>
    <tr>
        <th>after each item</th>
        <th>after every 10 items</th>
        <th>does not suspend <a href="#footnote-streams-benchmark">*</a></th>
    </tr>
    <tr>
        <td>producer -> consumer</td>
        <td>18M</td>
        <td>38M</td>
        <td>43M</td>
    </tr>
    <tr>
        <td>producer -> filter -> consumer (filter passes all items)</td>
        <td>16M</td>
        <td>36M</td>
        <td>42M</td>
    </tr>
    <tr>
        <td>producer -> filter -> ... -> filter -> consumer (10 filters in chain that pass all items)</td>
        <td>9M</td>
        <td>20M</td>
        <td>24M</td>
    </tr>
    <tr>
        <td>producer -> filter -> consumer (filter passes odd numbers)</td>
        <td>24M</td>
        <td>38M</td>
        <td>42M</td>
    </tr>
    <tr>
        <td>producer -> filter -> transformer -> consumer (filter passes all items, transformer returns an input number)</td>
        <td>16M</td>
        <td>34M</td>
        <td>40M</td>
    </tr>
    <tr>
        <td>producer -> splitter (2) -> consumer (2) (splitter splits an input stream into two streams)</td>
        <td>10M</td>
        <td>30M</td>
        <td>31M</td>
    </tr>
    <tr>
        <td>producer -> splitter (2) -> union (2) -> consumer (splitter first splits an input stream into two streams; union then merges this two streams back into a single stream)</td>
        <td>8M</td>
        <td>24M</td>
        <td>31M</td>
    </tr>
    <tr>
        <td>producer -> map -> map -> consumer (first mapper maps an input number into the key-value pair, the second one extracts back the value)</td>
        <td>16M</td>
        <td>31M</td>
        <td>36M</td>
    </tr>
    <tr>
        <td>producer -> map -> reducer -> map -> consumer (first mapper maps an input number into the key-value pair, reducer sums values by key (buffer size = 1024), the second mapper extracts back the values)</td>
        <td>12M</td>
        <td>19M</td>
        <td>20M</td>
    </tr>
    <tr>
        <td>producer -> streamBinarySerializer -> streamBinaryDeserializer -> consumer (serializer buffer size = 1024)</td>
        <td>12M</td>
        <td>22M</td>
        <td>24M</td>
    </tr>
</table>

<a name="footnote-streams-benchmark">\*</a> Typically, suspend/resume occurs very infrequently, only when consumers are saturated or during network congestions. In most cases intermediate buffering alleviates the suspend/resume cost and brings amortized complexity of your data processing pipeline to maximum throughput figures shown here.
