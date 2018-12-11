---
id: json
filename: json/
title: JSON Module
prev: modules/codegen.html
next: modules/rpc.html
---

JSON module allows to work with custom object transformations in a fast and efficient way. 

## Examples 
1. [Gson Adapters Example](https://github.com/softindex/datakernel-examples/blob/master/examples/json/src/main/java/io/datakernel/examples/GsonAdaptersExample.java) - converting a LocalDate object to JSON string and then recovering it back to LocalDate object with Gson Adapter.
2. [Type Adapter Object Example](https://github.com/softindex/datakernel-examples/blob/master/examples/json/src/main/java/io/datakernel/examples/GsonAdapterObjectExample.java) - setting a TypeAdapter for custom object Person to convert it to JSON string and then recovering it back. 
3. [Type Adapter Object Subtype Example](https://github.com/softindex/datakernel-examples/blob/master/examples/json/src/main/java/io/datakernel/examples/GsonAdapterObjectSubtypeExample.java) - setting a Type Adapter which can work with two custom subtypes: NameHolder and IntegerPersonHolder. Then converting custom objects to JSON string and recovering them back.

To run the examples, you should first execute these lines in the console in appropriate folder:
{% highlight bash %}
$ git clone https://github.com/softindex/datakernel-examples.git
$ cd datakernel-examples/examples/json
$ mvn clean package exec:java -Dexec.mainClass=io.datakernel.examples.GsonAdaptersExample
$ # OR
$ mvn clean package exec:java -Dexec.mainClass=io.datakernel.examples.TypeAdapterObjectExample
$ # OR
$ mvn clean package exec:java -Dexec.mainClass=io.datakernel.examples.TypeAdapterObjectSubtypeExample
{% endhighlight %}