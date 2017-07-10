package io.datakernel.storage.remote;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.datakernel.serializer.GsonSubclassesAdapter;

public class DataStorageRemoteCommands {
	static Gson commandGSON = new GsonBuilder()
			.registerTypeAdapter(RemoteCommand.class, GsonSubclassesAdapter.create()
					.withSubclassField("commandType")
					.withSubclass("GetSortedStream", GetSortedStream.class))
			.setPrettyPrinting()
			.enableComplexMapKeySerialization()
			.create();

	public static abstract class RemoteCommand {

	}

	public static final class GetSortedStream extends RemoteCommand {
		private final String predicateString;

		public GetSortedStream(String predicateString) {
			this.predicateString = predicateString;
		}

		public String getPredicateString() {
			return predicateString;
		}

		@Override
		public String toString() {
			return "GetSortedStream";
		}
	}

}
