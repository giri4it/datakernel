package io.datakernel.storage;

import io.datakernel.async.CompletionCallback;

public interface Consolidator {

	void consolidate(CompletionCallback callback);
}
