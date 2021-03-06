## Promise

Promise is an efficient replacement of default Java `CompletionStage` interface and represents partial computations of a 
large one. Promise will succeed (or fail) at some unspecified time and you should chain method calls that will be executed 
in both cases. These methods basically convert one promise into another.

* Compared to JavaScript, these promises are better optimized - intermediate promises are stateless and the promise 
graph executes with minimal garbage and overhead.
* Since Eventloop is single-threaded, so are Promises. That is why Promises are much more efficient comparing to Java's 
`CompletableFuture`.
* Promise module also contains utility classes that help to collect results of promises, add loops and conditional logic 
to promises execution.

[`Promise` interface](https://github.com/softindex/datakernel/blob/master/core-promise/src/main/java/io/datakernel/async/Promise.java) 
represents the main logic of the Promises.

### You can explore Promise examples [here](https://github.com/softindex/datakernel/tree/master/examples/promise)
