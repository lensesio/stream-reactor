# HTTP sink request timeout

`connect.http.request.timeout.ms` limits each HTTP request attempt to 30,000 ms by
default. Set it to a positive integer to allow a different duration:

```properties
connect.http.request.timeout.ms=30000
```

The timeout covers uploading the request, waiting for response headers, and
consuming the response body. Headers and body share one time budget. Increase
the value if the endpoint legitimately takes longer than 30 seconds.

This differs from `connect.http.connection.timeout.ms`, which limits connection
establishment. A server that accepts a connection but never responds is bounded
by the request timeout. OAuth token requests also use the shared HTTP client.

Each retry attempt receives a new timeout budget. Header/acquisition timeouts
follow the existing retry policy; a timeout while consuming the final response
body is reported as a request failure. Retry backoff is outside the per-attempt
budget, so the timeout does not limit the total duration across all retries.
