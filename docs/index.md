---
layout: default
title: DataKernel Framework
nav-menu: home
---

## Introduction

DataKernel is a full-stack application framework for Java. It contains components for building applications of different 
scales, from single-node HTTP server to large distributed systems. DataKernel was inspired by Node.js, so it's 
asynchronous, event-driven, lightweight and very easy to use. Moreover, it is completely free of legacy stuff, 
such as XML, Java EE, etc.

Due to the usage of modern asynchronous I/O, DataKernel is extremely fast, which is proven by benchmarks.

The essential components of DataKernel form the basis of our ad-serving infrastructure at [AdKernel](http://adkernel.com), 
running in production environments and processing billions of requests. Specifically, DataKernel is the foundation for 
systems that provide real-time analytics for ad publishers and advertisers, user data processing tools that we use for 
ad targeting, and also web crawlers that perform content indexing on a large scale.

## Current version:  **{{site.datakernel_version}}**

You can add DataKernel modules to your project by inserting dependency in pom.xml. For example add following lines to 
use the launcher module: 
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


## Core components
* [ByteBuf](/docs/modules/bytebuf.html) Memory-efficient, recyclable byte buffer as an alternative to Java's ByteBuffer 
class.
* [Eventloop](/docs/modules/eventloop.html) Efficient non-blocking network and file I/O, for building Node.js-like 
client/server applications with high performance requirements.
* [NET](/docs/modules/net.html) Handles low-level asynchronous socket I/O (TCP/UDP).
* [CSP](/docs/modules/csp.html) (Communicating Sequential Processes) - provides sequential communication 
between processes via channels similarly to the Go Lang.
* [Promise](/docs/modules/promise) An alternative to Java's CompletionStage, based on JavaScript Promise principles.
* [Codegen](/docs/modules/codegen.html) Expression-based fluent API on top of ObjectWeb ASM for runtime dynamic 
generation of classes and methods.
* [Serializer](/docs/modules/serializer.html) Extremely fast and space-efficient serializers, crafted using bytecode 
engineering. [Benchmark](/docs/modules/serializer.html#benchmark)
* [DataStream](/docs/modules/datastream.html) Composable asynchronous/reactive streams with powerful data processing 
capabilities. [Benchmark](/docs/modules/datastream.html#benchmark)
* [HTTP](/docs/modules/http.html) High-performance asynchronous HTTP client and server. 
[Benchmark](/docs/modules/http.html#benchmark)
* [JSON](/docs/modules/json.html) Contains tools for encoding/decoding primitives and objects. 

## Cloud components
* [RPC](/docs/modules/rpc.html) High-performance and fault-tolerant remote procedure call module for building distributed 
applications.
* [FS](/docs/modules/fs.html) Basis for building efficient, scalable remote file servers.
* [OT](/docs/modules/ot.html) Allows to create applications for collaborative editing based on Git-like approach with 
automatic conflict resolution.
* [OT-MySQL](/docs/modules/ot-mysql.html) Provides binding OT repositories to MySQL (or any other) database.
* [LSMT Table](/docs/modules/lsmt-table.html) Log-structured merge-tree table which stores aggregate functions and 
designed for OLAP workload.
* [LSMT Database](/docs/modules/lsmt-database.html) Multidimensional OLAP (Online Analytical Processing) database with 
predefined set of dimensions, measures, and LSMT tables.
* [Dataflow](/docs/modules/dataflow.html) Distributed stream-based batch processing engine for Big Data applications.
* [CRDT](/docs/modules/crdt.html) An alternative to Cluster-OT with CRDT (conflict-free replicated data type) approach.
* [Multilog](/docs/modules/multilog.html) Manages persistence of logs utilizing FS module.
* [ETL](/docs/modules/etl.html) (Extraction-transformation-loading) - near real-time async data processing system which
utilizes FS and OT modules.

## Global components
* [Global Common](/docs/modules/global-common.html) Foundation for other Global components.
* [Global-FS](/docs/modules/global-fs.html) A framework to create file sharing systems alternative to IPFS / BitTorrent 
technologies.
* [Global-OT](/docs/modules/global-ot.html) Extends both OT and blockchain technology, applicable on the global scale.

## Integration components
* [Boot](/docs/modules/boot.html) An intelligent way of booting complex applications and services according to their 
dependencies.
* [Launchers](/docs/modules/launchers.html) Module contains a set of predefined launchers as well as some common 
initializers for services
* [UIKernel](/docs/modules/uikernel.html) Integration with UIKernel.io JS frontend library: JSON serializers, grid model, 
basic servlets.
