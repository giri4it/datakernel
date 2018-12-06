---
id: rpc
filename: rpc/
title: RPC Module
prev: modules/codegen.html
next: modules/aggregation.html
---

RPC module is the framework to build distributed applications requiring efficient client-server interconnections between servers.

* Ideal to create near-realtime (i.e. memcache-like) servers with application-specific business logic
* Up to ~5.7M of requests per second on single core
* Pluggable high-performance asynchronous binary RPC streaming protocol
* Consistent hashing and round-robin distribution strategies
* Fault tolerance - with reconnections to fallback and replica servers

## Examples

1. ["Hello World" Client and Server](https://github.com/softindex/datakernel-examples/blob/master/examples/rpc/src/main/java/io/datakernel/examples/RpcExample.java)

To run the example, you should execute these three lines in the console in appropriate folder:
{% highlight bash %}
$ git clone https://github.com/softindex/datakernel-examples.git
$ cd datakernel-examples/examples/rpc
$ mvn clean package exec:java -Dexec.mainClass=io.datakernel.examples.RpcExample
{% endhighlight %}