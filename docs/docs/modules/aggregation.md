---
id: aggregation
filename: aggregation/
title: Aggregation Module
prev: modules/rpc.html
next: modules/remotefs.html
---

The Aggregation Module provides the foundation for building almost any type of database by providing the possibility to define custom aggregate functions.

## Features
* Log-Structured Merge Trees as core storage principle (unlike OLTP databases, it is designed from ground up for OLAP workload)
* Up to ~1.5M of inserts per second into aggregation on single core
* Aggregations storage medium can use any distributed file system
