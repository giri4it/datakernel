---
id: eventloop
filename: eventloop/
title: Eventloop Module
prev: modules/bytebuf.html
next: modules/http.html
---

Eventloop module is the foundation of other modules that run their code inside event loops and threads. Useful for building client-server applications with high performance requirements.

* Node.js-like approach for asynchronous I/O (TCP, UDP)
* Eliminates traditional bottleneck of I/O for further business logic processing
* Can run multiple event loop threads on available cores
* Minimal GC pressure: arrays and byte buffers are reused

## Examples
1. [TCP Echo Server](https://github.com/softindex/datakernel-examples/blob/master/examples/eventloop/src/main/java/io/datakernel/examples/TcpEchoServerExample.java)
2. [TCP Client](https://github.com/softindex/datakernel-examples/blob/master/examples/eventloop/src/main/java/io/datakernel/examples/TcpClientExample.java)
3. [TCP Multi Echo Server](https://github.com/softindex/datakernel-examples/blob/master/examples/eventloop/src/main/java/io/datakernel/examples/MultiEchoServerExample.java)

To run the examples, you should execute these three lines in the console in appropriate folder:
{% highlight bash %}
$ git clone https://github.com/softindex/datakernel-examples.git
$ cd datakernel-examples/examples/eventloop
$ mvn clean package exec:java -Dexec.mainClass=io.datakernel.examples.TcpEchoServerExample
$ # OR
$ mvn clean package exec:java -Dexec.mainClass=io.datakernel.examples.MultiEchoServerExample
{% endhighlight %}

This will start your echo server. No try connecting to it by executing these lines in appropriate folder:
{% highlight bash %}
$ cd datakernel-examples/examples/eventloop
$ mvn clean package exec:java -Dexec.mainClass=io.datakernel.examples.TcpClientExample
{% endhighlight %}

Note that your server procces shouldn't be terminated. If you started Multi Echo Server, feel free to connect multiple Tcp Clients and check how it works.