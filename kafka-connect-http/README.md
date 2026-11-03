# HTTP sink gzip request compression

Request compression is disabled by default. Enable it when the destination
accepts gzip-encoded HTTP request bodies:

```properties
connect.http.compression.type=gzip
```

The setting accepts `none` (default) and `gzip`, case-insensitively. Other values
are rejected during configuration validation.

With gzip enabled, the connector compresses the rendered UTF-8 request body,
sets `Content-Encoding: gzip`, and sends the compressed byte length as
`Content-Length`. Any configured `Content-Type` is preserved; otherwise the
existing default of `text/plain; charset=UTF-8` applies. Configured content
encoding and length headers are replaced by the values for the compressed body.

The compressed bytes are replayable across HTTP retries. Compression applies
to delivery payloads; OAuth token requests retain their existing encoding.
