---
id: codegen
filename: codegen/
title: Codegen Module
prev: modules/serializer.html
next: modules/rpc.html
---

Codegen module allows to build classes and methods in runtime without the overhead of reflection.

* Dynamically creates classes needed for runtime query processing (storing the results of computation, intermediate tuples, compound keys etc.)
* Implements basic relational algebra operations for individual items: aggregate functions, projections, predicates, ordering, group-by etc.
* Since I/O overhead is already minimal due to [Eventloop](/docs/modules/eventloop/) module, bytecode generation ensures that business logic (such as innermost loops processing millions of items) is also as fast as possible
* Easy to use API that encapsulates most of the complexity involved in working with bytecode

## Examples

1. [Dynamic Class Creation](https://github.com/softindex/datakernel-examples/blob/master/examples/codegen/src/main/java/io/datakernel/examples/DynamicClassCreationExample.java)

To run the example, you should execute these three lines in the console in appropriate folder:
{% highlight bash %}
$ git clone https://github.com/softindex/datakernel-examples.git
$ cd datakernel-examples/examples/codegen
$ mvn clean package exec:java -Dexec.mainClass=io.datakernel.examples.DynamicClassCreationExample
{% endhighlight %}
