---
id: launchers
filename: launchers/
title: Launchers Module
prev: modules/boot.html
next: modules/uikernel.html
---
Launchers are basically ready server applications. They use ServiceGraph to properly boot your application with all services and Google Guice to inject dependencies.
For some standard cases (HttpServer, RpcServer, RemoteFsServer, etc…) there is a variety of predefined launchers for you to use.