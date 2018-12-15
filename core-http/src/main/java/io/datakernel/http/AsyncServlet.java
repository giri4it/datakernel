/*
 * Copyright (C) 2015 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
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

package io.datakernel.http;

import io.datakernel.async.Promise;
import io.datakernel.exception.ParseException;
import io.datakernel.exception.UncheckedException;
import io.datakernel.util.ApplicationSettings;
import io.datakernel.util.MemSize;

/**
 * Servlet receives and responds to {@link HttpRequest} from clients across
 * HTTP. Receives {@link HttpRequest}, creates {@link HttpResponse} and sends
 * it.
 */
@FunctionalInterface
public interface AsyncServlet {
	Promise<HttpResponse> serve(HttpRequest request) throws ParseException, UncheckedException;

	MemSize DEFAULT_MAX_REQUEST_BODY = MemSize.of(
			ApplicationSettings.getInt(AsyncServlet.class, "maxRequestBody", 1024 * 1024));

	static AsyncServlet ensureRequestBody(AsyncServlet delegate) {
		return ensureRequestBody(delegate, DEFAULT_MAX_REQUEST_BODY);
	}

	static AsyncServlet ensureRequestBody(AsyncServlet delegate, MemSize maxBodySize) {
		return ensureRequestBody(delegate, maxBodySize.toInt());
	}

	static AsyncServlet ensureRequestBody(AsyncServlet delegate, int maxBodySize) {
		return request -> request.ensureBody(maxBodySize)
				.thenCompose(requestWithBody -> {
					try {
						return delegate.serve(requestWithBody);
					} catch (UncheckedException u) {
						return Promise.ofException(u.getCause());
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				});
	}

}