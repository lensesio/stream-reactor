# HTTP sink flush trigger metrics

The HTTP sink exposes the following cumulative JMX attributes on its existing
`HttpSinkMetrics` MBean:

| Attribute | Flush condition |
| --- | --- |
| `BatchCountTriggerCount` | The configured batch record-count limit was reached. |
| `BatchSizeTriggerCount` | The configured batch byte-size limit was reached or exceeded by a candidate record. |
| `TimeIntervalTriggerCount` | The configured interval elapsed, including timer-driven flushes with no new records. |

These counters record the conditions satisfied while selecting a batch. They
are non-exclusive: a submission can increment more than one counter when
multiple conditions are satisfied.

Counters increment after successful template rendering and immediately before
calling the HTTP sender. Failed HTTP submissions count; template failures do
not. HTTP client retries within a submission do not increment these counters
again, while at-least-once redelivery can create another counted submission.
They measure flush triggers rather than successful deliveries or individual
HTTP retry attempts.
