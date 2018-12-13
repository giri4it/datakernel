---
id: eventloop
filename: eventloop/
title: Eventloop
prev: modules/bytebuf.html
next: modules/net.html
---

Eventloop module is the foundation of other modules that run their code inside event loops and threads. Useful for building client-server applications with high performance requirements.

* Node.js-like approach for asynchronous I/O (TCP, UDP)
* Eliminates traditional bottleneck of I/O for further business logic processing
* Can run multiple event loop threads on available cores
* Minimal GC pressure: arrays and byte buffers are reused

## Examples
1. [Busy Wait Eventloop Echo Server]() - poor implementation of echo server at is looping infinitely while trying to data from socket.
2. [Selector Eventloop Echo Server]() -

To run the examples, you should execute these three lines in the console in appropriate folder:
{% highlight bash %}
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel/examples/eventloop
$ mvn clean compile exec:java@BusyWaitEventloopEchoServer
$ # OR
$ mvn clean compile exec:java@SelectorEventloopEchoServer
{% endhighlight %}
