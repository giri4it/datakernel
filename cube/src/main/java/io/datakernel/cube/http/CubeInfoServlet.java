/*
 * Copyright (C) 2015 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.cube.http;

import com.google.gson.Gson;
import io.datakernel.async.ForwardingResultCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.cube.CubeQuery;
import io.datakernel.cube.ICube;
import io.datakernel.cube.QueryResult;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.datakernel.cube.http.Utils.createGsonBuilder;

final class CubeInfoServlet implements AsyncServlet {
	private static final Logger logger = LoggerFactory.getLogger(CubeInfoServlet.class);
	private final Gson gson;
	private final ICube cube;

	private CubeInfoServlet(ICube cube, Gson gson) {
		this.cube = cube;
		this.gson = gson;
	}

	@Override
	public void serve(HttpRequest request, final ResultCallback<HttpResponse> callback) {
		try {
			final CubeQuery cubeQuery = CubeQuery.create().fetchCubeInfo();
			cube.query(cubeQuery, new ForwardingResultCallback<QueryResult>(callback) {
				@Override
				protected void onResult(QueryResult result) {
					String json = gson.toJson(result);
					HttpResponse httpResponse = ReportingServiceServlet.createResponse(json);
					callback.setResult(httpResponse);
				}
			});
		} catch (Exception e) {
			logger.error("Exception while attempting to get cube info", e);
			callback.setException(e);
		}
	}

	public static AsyncServlet create(ICube cube) {
		Gson gson = createGsonBuilder(cube.getAttributeTypes(), cube.getMeasureTypes()).create();
		return new CubeInfoServlet(cube, gson);
	}

}
