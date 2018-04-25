package io.datakernel.stream;

public enum StreamCapability {
	/**
	 * Indicates that the stream can be bound outside current eventloop tick.
	 */
	LATE_BINDING,
	/**
	 * Indicates that the stream guarantees that it will stop producing items immediately after calling suspend.
	 */
	IMMEDIATE_SUSPEND,
	/**
	 * Indicates that the stream always forwards its suspend calls to some other stream.
	 * This is used to reduce logging of suspend calls.
	 */
	SUSPEND_CALL_FORWARDER,
	/**
	 * Indicates that the stream always forwards its produce calls to some other stream.
	 * This is used to reduce logging of produce calls.
	 */
	PRODUCE_CALL_FORWARDER
}