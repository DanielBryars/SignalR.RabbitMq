SignalR.RabbitMq
================

About
-----
SignalR.RabbitMq is an implementation of an IMessageBus using RabbitMq as the backing store and would be used to allow a
signalr web application to be scaled across a web farm.


Please note the following describes useage of the release built on SignalR 1.0.0-alpha
--------------------------------------------------------------------------------------


Installation
------------

A compiled library is available via NuGet

To install via the nuget package console

```PS
Install-Package SignalR.RabbitMq -Pre
```

To install via the nuget user interface in Visual Studio the package to search for is "SignalR.RabbitMq"


Useage
------

General Usage
-------------

The example web project shows how you could configure the message bus in the global.asax.cs file.

```CSHARP
var factory = new ConnectionFactory 
		{ 
			UserName = "guest",
			Password = "guest"
		};

var exchangeName = "SignalRExchange";
GlobalHost.DependencyResolver.UseRabbitMq(factory, exchangeName);
```

The SignalR.RabbitMq message bus expects to be handed an instance of a configured ConnectionFactory as produced by the RabbitMq.Client and the name of a message exchange to be used for the signalr messages.

The message bus will then create the exchange if it does not already exist then listen on an anonymous queue for messages across the web farm. There will be one queue per server in the web farm.

The message exchange should only be used for signalr messages.


Send to client via message bus
------------------------------

So you have scaled out your message bus and scaled out SignalR something awesome but what next?

It might be interesting to send messages directly to connected clients from another process.

From the SignalR.RabbitMQ.Console project -

```CSHARP

var factory = new ConnectionFactory
{
	UserName = "guest",
	Password = "guest"
};

var exchangeName = "SignalRExchange";
GlobalHost.DependencyResolver.UseRabbitMq(factory, exchangeName);

var hubContext = GlobalHost.ConnectionManager.GetHubContext<Chat>();

Task.Factory.StartNew(
	() =>
		{
			while (true)
			{
				hubContext.Clients.All.onConsoleMessage("Hello!");
				Thread.Sleep(1000);
			}
		}
	);

```

The onConsoleMessage method is a javascript function on the client.
The message "Hello!" is put onto the message bus and relayed by the web application to the connected clients.


FAQ
---

The library uses the RabbitMq.Client for better or worse.