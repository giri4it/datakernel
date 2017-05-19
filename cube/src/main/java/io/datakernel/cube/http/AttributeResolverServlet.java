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

import com.google.common.base.Charsets;
import com.google.gson.Gson;
import io.datakernel.aggregation.AggregationPredicate;
import io.datakernel.aggregation.QueryException;
import io.datakernel.async.ForwardingResultCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.cube.Cube;
import io.datakernel.cube.CubeQuery;
import io.datakernel.cube.QueryResult;
import io.datakernel.exception.ParseException;
import io.datakernel.http.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.datakernel.bytebuf.ByteBufStrings.wrapUtf8;
import static io.datakernel.cube.http.Utils.*;

public class AttributeResolverServlet implements AsyncServlet {
	private static final Logger logger = LoggerFactory.getLogger(AttributeResolverServlet.class);

	private final Cube cube;
	private final Gson gson;

	private AttributeResolverServlet(Cube cube, Gson gson) {
		this.cube = cube;
		this.gson = gson;
	}

	public static AttributeResolverServlet create(Cube cube) {
		Gson gson = createGsonBuilder(cube.getAttributeTypes(), cube.getMeasureTypes()).create();
		return new AttributeResolverServlet(cube, gson);
	}

	@Override
	public void serve(final HttpRequest httpRequest, final ResultCallback<HttpResponse> callback) {
		try {
			final CubeQuery cubeQuery = parseQuery(httpRequest);
			cube.resolveAttributes(cubeQuery, new ForwardingResultCallback<QueryResult>(callback) {
				@Override
				protected void onResult(QueryResult result) {
					String json = gson.toJson(result);
					HttpResponse httpResponse = createResponse(json);
					logger.info("Processed request {} ({}) ", httpRequest, cubeQuery);
					callback.setResult(httpResponse);
				}
			});
		} catch (ParseException e) {
			logger.error("Parse exception: " + httpRequest, e);
			callback.setException(e);
		} catch (QueryException e) {
			logger.error("Query exception: " + httpRequest, e);
			callback.setException(e);
		}
	}

	private static HttpResponse createResponse(String body) {
		HttpResponse response = HttpResponse.ok200();
		response.setContentType(ContentType.of(MediaTypes.JSON, Charsets.UTF_8));
		response.setBody(wrapUtf8(body));
		response.addHeader(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
		return response;
	}

	CubeQuery parseQuery(HttpRequest request) throws ParseException {
		CubeQuery query = CubeQuery.create();

		String parameter;

		parameter = request.getParameter(ATTRIBUTES_PARAM);
		if (parameter != null)
			query = query.withAttributes(SPLITTER.splitToList(parameter));

		parameter = request.getParameter(WHERE_PARAM);
		if (parameter != null)
			query = query.withWhere(gson.fromJson(parameter, AggregationPredicate.class));

		parameter = request.getParameter(RESOLVE_ATTRIBUTES_PARAM);
		if(parameter != null && Boolean.parseBoolean(parameter))
			query = query.withResolveAttributes();

		return query;
	}
}
