Use [zerolog] as an example of use.

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

run main and access it with the following address:

http://localhost:9191/test/display?level=warn&count=100&guc=10
