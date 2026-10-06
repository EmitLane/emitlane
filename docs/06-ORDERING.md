# Ordering model

Ordered delivery is opt-in per `(destination, ordering_key)`. Applications own
positive domain sequences; gaps, retries and dead expected events block later
sequences. Delivery remains at least once and consumers still deduplicate.

The producer contract, stream start, Kafka key affinity, 64 virtual partitions,
leases, epoch fencing, timing assumptions, replay and upgrade procedures are
maintained in [Ordered delivery](ORDERED_DELIVERY.md). This entry point remains
available for existing links.
