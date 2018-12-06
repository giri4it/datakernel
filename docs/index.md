---
layout: default
title: DataKernel Framework
nav-menu: home
---

## Introduction

DataKernel is a full-stack application framework for Java. It contains components for building applications of different scales, from single-node HTTP server to large distributed systems. DataKernel was inspired by Node.js, so it's asynchronous, event-driven, lightweight and very easy to use. Moreover, it is completely free of legacy stuff, such as XML, Java EE, etc.

Due to the usage of modern asynchronous I/O, DataKernel is extremely fast, which is proven by benchmarks.

The essential components of DataKernel form the basis of our ad-serving infrastructure at [AdKernel](http://adkernel.com), running in production environments and processing billions of requests. Specifically, DataKernel is the foundation for systems that provide real-time analytics for ad publishers and advertisers, user data processing tools that we use for ad targeting, and also web crawlers that perform content indexing on a large scale.

## Current version:  **{{site.datakernel_version}}**

You can add DataKernel modules to your project by inserting dependency in pom.xml. For example add following lines to use the launcher module: 
{% highlight xml %}
<dependency>
    <groupId>io.datakernel</groupId>
    <artifactId>datakernel-launchers</artifactId>
    <version>{{site.datakernel_version}}</version>
</dependency>
{% endhighlight %}

If you want to clone DataKernel git repository and build it locally or edit it, run the following commands:
{% highlight bash %}
$ git clone https://github.com/softindex/datakernel
$ cd datakernel
$ git checkout v{{site.datakernel_version}}
$ mvn clean install -DskipTests=true
{% endhighlight %}

## Foundation components

* [ByteBuf](/docs/modules/bytebuf.html) Memory-efficient, recyclable byte buffers.
* [Eventloop](/docs/modules/eventloop.html) Efficient non-blocking network and file I/O, for building Node.js-like client/server applications with high performance requirements.

## Core components

* [HTTP](/docs/modules/http.html) High-performance asynchronous HTTP client and server. [Benchmark](/docs/modules/http.html#benchmark)
* [DataStreams](/docs/modules/streams.html) Composable asynchronous/reactive streams with powerful data processing capabilities. [Benchmark](/docs/modules/streams.html#benchmark)
* [Codegen](/docs/modules/codegen.html) Expression-based fluent API on top of ObjectWeb ASM for runtime generation of POJOs, mappers and reducers, etc.
* [Serializer](/docs/modules/serializer.html) Extremely fast and space-efficient serializers, crafted using bytecode engineering by Codegen module. [Benchmark](/docs/modules/serializer.html#benchmark)

## Cluster components

* [RPC](/docs/modules/rpc.html) High-performance and fault-tolerant remote procedure call module for building distributed applications.
* [Aggregation](/docs/modules/aggregation.html) Unique database with the possibility to define custom aggregation functions.
* [RemoteFS](/docs/modules/remotefs.html) Basis for building efficient, scalable remote file servers.

## Integration components
 
* [Boot](/docs/modules/boot.html) An intelligent way of booting complex applications and services according to their dependencies.
* [UIKernel](/docs/modules/uikernel.html) Integration with UIKernel.io JS frontend library: JSON serializers, grid model, basic servlets.