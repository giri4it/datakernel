---
id: remotefs
filename: remotefs/
title: RemoteFS Module
prev: modules/aggregation.html
next: modules/boot.html
---

RemoteFS Module is basis for building efficient, scalable remote file servers.

## Examples

1. [Server Setup](https://github.com/softindex/datakernel-examples/blob/master/examples/remotefs/src/main/java/io/datakernel/examples/ServerSetupExample.java)
2. [File Upload](https://github.com/softindex/datakernel-examples/blob/master/examples/remotefs/src/main/java/io/datakernel/examples/FileUploadExample.java)
3. [File Download](https://github.com/softindex/datakernel-examples/blob/master/examples/remotefs/src/main/java/io/datakernel/examples/FileDownloadExample.java)

To run the examples, you should execute these three lines in the console in appropriate folder:
{% highlight bash %}
$ git clone https://github.com/softindex/datakernel-examples.git
$ cd datakernel-examples/examples/remotefs
$ mvn clean package exec:java -Dexec.mainClass=io.datakernel.examples.ServerSetupExample
$ # OR
$ mvn clean package exec:java -Dexec.mainClass=io.datakernel.examples.FileUploadExample
$ # OR
$ mvn clean package exec:java -Dexec.mainClass=io.datakernel.examples.FileDownloadExample
{% endhighlight %}

Note that to work properly all those three examples should be launched in order given here.