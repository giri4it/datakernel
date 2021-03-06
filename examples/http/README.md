1. [HTTP Server Example](https://github.com/softindex/datakernel/blob/master/examples/http/src/main/java/io/datakernel/examples/HttpServerExample.java) - 
a simple HTTP Server utilizing `HttpServerLauncher`.
2. [HTTP Multithreaded Server Example](https://github.com/softindex/datakernel/blob/master/examples/http/src/main/java/io/datakernel/examples/HttpMultithreadedServerExample.java) - 
HTTP multithreaded server example.
3. [HTTP Client Example](https://github.com/softindex/datakernel/blob/master/examples/http/src/main/java/io/datakernel/examples/HttpClientExample.java) - 
an HTTP client example utilizing `Launcher`.
4. [Middleware Servlet Example](https://github.com/softindex/datakernel/blob/master/examples/http/src/main/java/io/datakernel/examples/MiddlewareServletExample.java) - 
an example of `MiddlewareServlet` usage.
5. [Request Parameter Example](https://github.com/softindex/datakernel/blob/master/examples/http/src/main/java/io/datakernel/examples/RequestParameterExample.java) - 
an example of processing requests with parameter.
6. [Static Servlet Example](https://github.com/softindex/datakernel/blob/master/examples/http/src/main/java/io/datakernel/examples/StaticServletExample.java) - 
an example of `StaticServlet` utilizing.

To run the examples, you should execute these lines in the console in appropriate folder:
```
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel/examples/http
$ mvn clean compile exec:java@HttpServerExample
$ # or
$ mvn clean compile exec:java@HttpMultithreadedServerExample
$ # or
$ mvn clean compile exec:java@MiddlewareServletExample
$ # or
$ mvn clean compile exec:java@RequestParametrExample
$ # or
$ mvn clean compile exec:java@StaticServletExample
```
To check how **HTTP Server Example** or **HTTP Multithreaded Server Example** works, you should start your client:
```
$ mvn clean compile exec:java@HttpClientExample
```

If you connected to the multithreaded server, you'll receive a message representing which worker processed your request:
```
"Hello from worker server #..." 
```
Otherwise, you'll see a message: `"Hello World!"`

The difference between **HTTP Multithreaded Server Example** and **HTTP Server Example** is that the first one creates 
several worker threads for requests processing. 

`HttpServerExample` utilizes `HttpServerLauncher` while `HttpMultithreadedServerExample` extends 
`MultithreadedHttpServerLauncher`. When using predefined launchers, you should override the following methods:
 * `getBusinessLogicModules()` - to specify the actual logic of your application
 * `getOverrideModules()` - to override default modules.

<br>

If you run Examples 4-6, you can connect to you server by visiting [localhost:8080](http://localhost:8080/) in your browser.

**Middleware Servlet Example** processes requests and redirects you to chosen web-page. You can set up configurations for your 
Servlet with method `with()`. It sets up HTTP methods (optionally), paths and `AsyncServlet`.

**Request Parameter Example** represents requests with parameters which are received with `getPostParameters()` and then 
utilized with `postParameters.get()`.

**Static Servlet Example** shows how to set up and utilize `StaticServlet`. Method `StaticServlet.create()` returns a 
new `StaticServlet`. 

