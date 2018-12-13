---
id: fs
filename: fs/
title: FS
prev: modules/rpc.html
next: modules/ot.html
---

RemoteFS Module is basis for building efficient, scalable remote file servers.

## Examples

1. [Server Setup](https://github.com/softindex/datakernel/blob/master/examples/remotefs/src/main/java/io/datakernel/examples/ServerSetupExample.java)
2. [File Upload](https://github.com/softindex/datakernel/blob/master/examples/remotefs/src/main/java/io/datakernel/examples/FileUploadExample.java)
3. [File Download](https://github.com/softindex/datakernel/blob/master/examples/remotefs/src/main/java/io/datakernel/examples/FileDownloadExample.java)

To run the examples, you should execute these lines in the console in appropriate folder:
{% highlight bash %}
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel/examples/remotefs
$ mvn clean compile exec:java@ServerSetupExample
$ # OR
$ mvn clean compile exec:java@FileUploadExample
$ # OR
$ mvn clean compile exec:java@FileDownloadExample
{% endhighlight %}

Note that to work properly all these three examples should be launched in order given here.