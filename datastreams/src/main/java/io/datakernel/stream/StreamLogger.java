package io.datakernel.stream;

import io.datakernel.stream.StreamLoggers.NoopStreamLoggerImpl;
import io.datakernel.stream.StreamLoggers.SimpleStreamLoggerImpl;
import io.datakernel.stream.StreamLoggers.StreamType;
import org.slf4j.Logger;

import java.util.Set;

import static io.datakernel.stream.StreamCapability.PRODUCE_CALL_FORWARDER;
import static io.datakernel.stream.StreamCapability.SUSPEND_CALL_FORWARDER;
import static io.datakernel.stream.StreamLoggers.defaultTag;

public interface StreamLogger {

	void logOpen();

	void logProduceRequest();

	void logSuspendRequest();

	void logClose();

	void logCloseWithError(Throwable throwable);

	StreamLogger createChild(StreamProducer stream, String childTag);

	StreamLogger createChild(StreamConsumer stream, String childTag);

	String getTag();

	static StreamLogger of(Logger logger, Object transformer) {
		return new SimpleStreamLoggerImpl(StreamType.TRANSFORMER, logger, defaultTag(transformer), true, true);
	}

	static StreamLogger of(StreamProducer<?> stream, Logger logger, String tag) {
		Set<StreamCapability> capabilities = stream.getCapabilities();
		return new SimpleStreamLoggerImpl(StreamType.PRODUCER, logger, tag,
			!capabilities.contains(PRODUCE_CALL_FORWARDER),
			!capabilities.contains(SUSPEND_CALL_FORWARDER));
	}

	static StreamLogger of(StreamConsumer<?> stream, Logger logger, String tag) {
		Set<StreamCapability> capabilities = stream.getCapabilities();
		return new SimpleStreamLoggerImpl(StreamType.CONSUMER, logger, tag,
			!capabilities.contains(PRODUCE_CALL_FORWARDER),
			!capabilities.contains(SUSPEND_CALL_FORWARDER));
	}

	static StreamLogger of(StreamProducer<?> stream, Logger logger) {
		return of(stream, logger, defaultTag(stream));
	}

	static StreamLogger of(StreamConsumer<?> stream, Logger logger) {
		return of(stream, logger, defaultTag(stream));
	}

	static StreamLogger noop(String tag) {
		return new NoopStreamLoggerImpl(tag);
	}

	static StreamLogger noop(StreamProducer stream) {
		return new NoopStreamLoggerImpl(defaultTag(stream));
	}

	static StreamLogger noop(StreamConsumer stream) {
		return new NoopStreamLoggerImpl(defaultTag(stream));
	}

	static StreamLogger noop(StreamProducer stream, String tag) {
		return new NoopStreamLoggerImpl(tag);
	}

	static StreamLogger noop(StreamConsumer stream, String tag) {
		return new NoopStreamLoggerImpl(tag);
	}
}
