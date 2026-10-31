-- pgbroadcaster.publish
-- Notifies the channel of the payload, which is an event as JSON
SELECT pg_notify(@channel, @payload)
;
