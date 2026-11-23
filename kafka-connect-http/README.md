# HTTP sink response-code counters

The existing per-task HTTP metrics MBean exposes cumulative `Http<code>Count`
attributes, such as `Http200Count`, `Http429Count`, and `Http503Count`:

```text
io.lenses.streamreactor.connect.http.sink:type=metrics,name=<connector>,task=<task>
```

Dedicated attributes cover these status codes:

```text
200 201 202 204 206
300 301 302 303 304 307 308
400 401 403 404 405 408 409 410 412 413 415 422 429
500 501 502 503 504 507 508 510 511
```

`OtherHttpStatusCodeCount` counts responses with any other status code. Each
successfully decoded final delivery response increments its code counter (or
the fallback) and the existing `2xxCount`, `4xxCount`, `5xxCount`, or
`OtherErrorsCount` bucket. As before, the last bucket includes statuses outside
the 2xx, 4xx, and 5xx ranges.

Intermediate responses retried by the HTTP client do not increment these final
response counters. OAuth token responses, transport failures, and failures
while decoding the response body do not increment them either. Counters are
cumulative for the metrics instance and survive request-latency histogram resets.
