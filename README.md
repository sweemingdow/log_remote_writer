Core Interface Definition
```go
type RemoteWriter interface {
	io.Writer

	// stop gracefully
	Stop(ctx context.Context) error

	// return monitor info
	Monitor() map[string]any
}
```

Use [[zerolog]](https://github.com/rs/zerolog) as an example of use.

Enter examples, modify the configuration.
```
remoteWriter := httpwriter.New(httpwriter.HttpRemoteConfig{
		Url:                   "http://192.168.1.155:9088", // your log forwarding server address (fluent-bit, fluentd ...)
		Workers:               16,
		BatchQuantitativeSize: 50,
		QueueSize:             500,
		Debug:                 false,
	})
```

To switch from http to tcp implementation, the only thing to do is to change the configuration
```go
remoteWriter := tcpwriter.New(tcpwriter.TcpRemoteConfig{
		Host:                      "192.168.1.155", // my fluent-bit server
		Port:                      5170,
		QueueSize:                 2000,
		Debug:                     false,
		DisplayMonitorIntervalSec: 15, // display monitor metrics
	})
```
run main and access it with the following address:

http://localhost:9191/test/display?level=warn&count=100&guc=10

#### Key Features

- **Universal Compatibility:** Uses io.Writer for broad support.

- **Remote Forwarding:** Efficiently forwards structured logs to a remote collector.

- **Protocol Support:** Connects to remote services via both TCP and HTTP.

- **Ideal for Collectors:** Perfectly suited for integrating with log aggregation systems like Fluent Bit and Fluentd.

It serves as a reliable middle layer to ensure your application logs are delivered asynchronously and reliably to your centralized log management system.

- how to choice
  - httpWriter

    Simple to use, takes up slightly more resources, decent performance (enough in most scenarios), and has good compatibility, especially with third-party systems.
  - tcpWriter
  
    Excellent performance, low resource usage. Huge throughput

  Final recommendations
    The system uses tcpWriter internally, and httpWriter is used to connect to external systems.
    
- how to config

  Except for queue capacity, the default value can be used in most cases