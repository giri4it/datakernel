---
id: promise
filename: /promise
title: Promise
prev: modules/csp.html
next: modules/codegen.html
---

Async Module includes Promise - an efficient replacement of default Java CompletionStage interface. 

## Examples
1. [Promises Example]() - some basic functionality of Promises.
2. [Async File Example]() - an example of asynchronous work with a text file using Promise.

To run the examples, you should execute these lines in the console in appropriate folder:
{% highlight bash %}
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel/examples/async
$ mvn clean compile exec:java@PromisesExample
$ #or
$ mvn mvn clean compile exec:java@AsyncFileExample
{% endhighlight %}