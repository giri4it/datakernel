---
id: boot
filename: boot/
title: Boot
prev: modules/global-ot.html
next: modules/launchers.html
---

Boot module enables booting complex applications and services according to their dependencies in intelligent way.

* Configuration: easy-to-use and flexible configs managing
* Service Graph: services manager to efficiently start/stop services, with respect to their dependencies
* Worker Pool: simply create worker pools for your applications and set the amount of workers.
* Guice integration: extension of [Google Guice](https://github.com/google/guice) Dependency Injection Framework to simplify work with service graph
* Launcher: utility to facilitate app launching using configs, service graph and guice


## Examples
1. [Config Module Example](https://github.com/softindex/datakernel/tree/master/examples/boot/src/main/java/io/datakernel/examples/ConfigModuleExample) - supplies config to your application and controls it.
2. [Service Graph Module Example](https://github.com/softindex/datakernel/tree/master/examples/boot/src/main/java/io/datakernel/examples/ConfigModuleExample) - manages a service which displays "Hello World!" message.
3. [Worker Pool Module Example](https://github.com/softindex/datakernel/tree/master/examples/boot/src/main/java/io/datakernel/examples/ConfigModuleExample) - creating a Worker Pool with 4 workers.

To run the examples, you should execute these lines in the console in appropriate folder:
{% highlight bash %}
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel/examples/boot
$ mvn clean compile exec:java@ConfigModuleExample
$ #or
$ mvn clean compile exec:java@ServiceGraphModuleExample
$ #or
$ mvn clean compile exec:java@WorkerPoolModuleExample
{% endhighlight %}