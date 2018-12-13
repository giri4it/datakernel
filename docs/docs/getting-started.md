---
id: getting-started
filename: getting-started
title: Getting Started
prev: installation.html
next: tutorial/
redirect_from: "docs/index.html"
---

## Purpose
In this guide we will create a simple “Hello World” application using [Eventloop](/docs/modules/eventloop/), which is the core component of Datakernel Framework.

## What you will need:

* JDK 1.8 or higher
* Maven 3.0 or higher

## To proceed with this guide you have 2 options:

* Download and run [working example](#working-example)
* Follow [step-by-step guide](#step-by-step)

## 1. Working Example {#working-example}

To run the complete example, enter next commands:
{% highlight bash %}
$ git clone https://github.com/softindex/datakernel
$ cd datakernel/examples/getting-started
$ mvn clean compile exec:java@HelloWorld
{% endhighlight %}

## 2. Step-by-step guide {#step-by-step}

Firstly, create a folder for application and build an appropriate project structure:
{% highlight xml %}
helloworld
└── pom.xml
└── src
    └── main
        └── java
            └── io
                └── datakernel
                    └── examples
                        └── HelloWorld.java
{% endhighlight %}

You can create this project structure manually or simply use the commands below:
{% highlight bash %}
$ mkdir -p helloworld/src/main/java/io/datakernel/examples
$ touch helloworld/pom.xml
$ touch helloworld/src/main/java/io/datakernel/examples/HelloWorld.java
{% endhighlight %}

Add a maven dependency to use DataKernel in your project, as showed below:
{% highlight xml %}
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>io.datakernel</groupId>
    <artifactId>helloworld</artifactId>
    <version>2.0</version>
    <packaging>jar</packaging>

    <name>HelloWorld</name>

    <dependencies>
        <dependency>
            <groupId>io.datakernel</groupId>
            <artifactId>datakernel-eventloop</artifactId>
            <version>{{site.datakernel_version}}</version>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.7.0</version>
                <configuration>
                    <source>1.8</source>
                    <target>1.8</target>
                    <encoding>UTF-8</encoding>
                </configuration>
            </plugin>
        </plugins>
    </build>

</project>
{% endhighlight %}

Then, write down the following code to HelloWorld.java
{% highlight java %}
package io.datakernel.examples;

import io.datakernel.eventloop.Eventloop;

public class HelloWorld {

	public static void main(String[] args) {
        //creating an eventloop and setting a task for it

		Eventloop eventloop = Eventloop.create();

		eventloop.post(() -> System.out.println("Hello World"));
        
        //starting the created eventlloop
		eventloop.run();
	}
}
{% endhighlight %}

Finally, enter the command below in console to compile and run this app:
{% highlight bash %}
$ mvn clean package exec:java -Dexec.mainClass=io.datakernel.examples.HelloWorld
{% endhighlight %}
