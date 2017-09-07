package io.datakernel.storage.remote;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.datakernel.serializer.GsonSubclassesAdapter;

class RemoteResponses {
	static Gson responseGson = new GsonBuilder()
			.registerTypeAdapter(RemoteResponse.class, GsonSubclassesAdapter.create()
					.withSubclassField("commandType")
					.withSubclass("OkResponse", OkResponse.class))
			.setPrettyPrinting()
			.enableComplexMapKeySerialization()
			.create();

	static abstract class RemoteResponse {}

	public static class OkResponse extends RemoteResponse {
		@Override
		public String toString() {
			return "Operation{OK}";
		}
	}
}
