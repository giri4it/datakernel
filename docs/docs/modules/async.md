---
id: async
filename: /async
title: Async Module
prev: modules/eventloop.html
next: modules/csp.html
---

Async Module includes Promise - an efficient replacement of default Java CompletionStage interface. 

## Examples
1. [Promises Example]() - some basic functionality of Promises.
2. [Async File Example]() - an example of asynchronous work with a text file using Promise.

To run the examples, you should execute these lines in the console in appropriate folder:
{% highlight bash %}
$ git clone https://github.com/softindex/datakernel-examples.git
$ cd datakernel-examples/examples/async
$ mvn clean package exec:java -Dexec.mainClass=io.datakernel.examples.PromiseExample
$ #or
$ mvn clean package exec:java -Dexec.mainClass=io.datakernel.examples.AsyncFileExample
{% endhighlight %}