/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class MiddlewareServlet implements AsyncServlet {
	private static final String ROOT = "/";

	protected final Map<String, MiddlewareServlet> routes = new HashMap<>();
	protected AsyncServlet fallbackServlet;

	protected final Map<HttpMethod, AsyncServlet> rootServlets = new HashMap<>();
	protected AsyncServlet rootServlet;

	protected final Map<String, MiddlewareServlet> parameters = new HashMap<>();

	private MiddlewareServlet() {
	}

	public static MiddlewareServlet create() {
		return new MiddlewareServlet();
	}

	public MiddlewareServlet with(String path, AsyncServlet servlet) {
		return with(null, path, servlet);
	}

	public MiddlewareServlet with(@Nullable HttpMethod method, String path, AsyncServlet servlet) {
		if (servlet == null)
			throw new NullPointerException();
		if (!path.isEmpty() && !path.startsWith(ROOT))
			throw new IllegalArgumentException("Invalid path " + path);
		if (path.isEmpty() || path.equals(ROOT)) {
			apply(method, servlet);
		} else {
			int slash = path.indexOf('/', 1);
			String remainingPath;
			String urlPart;
			if (slash == -1) {
				remainingPath = "";
				urlPart = path.substring(1);
			} else {
				remainingPath = path.substring(slash);
				urlPart = path.substring(1, slash);
			}
			MiddlewareServlet container = ensureMServlet(urlPart);
			if (urlPart.endsWith("*")) {
				assert "".equals(remainingPath) : "Tail-parameter can only be the last path part";
				container.with(method, remainingPath, create().withFallback(servlet));
			} else {
				container.with(method, remainingPath, servlet);
			}
		}
		return this;
	}

	public MiddlewareServlet withFallback(AsyncServlet servlet) {
		if (servlet == null)
			throw new NullPointerException();
		if (this.fallbackServlet != null)
			throw new IllegalStateException("Fallback servlet is already set");
		this.fallbackServlet = servlet;
		return this;
	}

	public MiddlewareServlet withFallback(String path, AsyncServlet servlet) {
		if (!path.isEmpty() && !path.startsWith(ROOT))
			throw new IllegalArgumentException("Invalid path " + path);
		if (path.isEmpty() || path.equals(ROOT)) {
			withFallback(servlet);
		} else {
			int slash = path.indexOf('/', 1);
			String remainingPath;
			String urlPart;
			if (slash == -1) {
				remainingPath = "";
				urlPart = path.substring(1);
			} else {
				remainingPath = path.substring(slash);
				urlPart = path.substring(1, slash);
			}
			MiddlewareServlet container = ensureMServlet(urlPart);
			container.withFallback(remainingPath, servlet);
		}
		return this;
	}

	@NotNull
	@Override
	public Promise<HttpResponse> serve(@NotNull HttpRequest request) {
		Promise<HttpResponse> processed = tryServeAsync(request);
		if (processed == null) {
			return Promise.ofException(HttpException.notFound404());
		}
		return processed;
	}

	@Nullable
	protected Promise<HttpResponse> tryServeAsync(HttpRequest request) {
		int introPosition = request.getPos();
		String urlPart = request.pollUrlPart();
		HttpMethod method = request.getMethod();

		if (urlPart.isEmpty()) {
			AsyncServlet servlet = getRootServletOrWildcard(method);
			if (servlet != null) {
				return servlet.serve(request);
			} else if (fallbackServlet == null) {
				if (!rootServlets.isEmpty()) {
					return null;
				}
			}
		}

		Promise<HttpResponse> result = null;

		MiddlewareServlet transit = routes.get(urlPart);
		if (transit != null) {
			result = transit.tryServeAsync(request);
		} else {
			int position = request.getPos();
			for (Entry<String, MiddlewareServlet> entry : parameters.entrySet()) {
				String key = entry.getKey();
				if (key.endsWith("*")) {
					String relativePath = request.getRelativePath();
					String tail = urlPart + (relativePath.isEmpty() ? "" : '/' + relativePath);
					request.putPathParameter(key.substring(0, key.length() - 1), tail);
					// *-template is asserted to be the last url part (at line 68), so we dont touch request.setPos
				} else {
					request.putPathParameter(key, urlPart);
				}
				result = entry.getValue().tryServeAsync(request);
				if (result != null) {
					return result;
				} else {
					request.removePathParameter(key);
					request.setPos(position);
				}
			}
		}

		if (result == null && fallbackServlet != null) {
			request.setPos(introPosition);
			result = fallbackServlet.serve(request);
		}
		return result;
	}

	private AsyncServlet getRootServletOrWildcard(HttpMethod method) {
		AsyncServlet servlet = rootServlets.get(method);
		if (servlet == null) {
			return rootServlet;
		}
		return servlet;
	}

	private void apply(@Nullable HttpMethod method, AsyncServlet servlet) {
		if (servlet instanceof MiddlewareServlet) {
			merge(this, (MiddlewareServlet) servlet);
		} else if (servlet instanceof WithMiddleware) {
			merge(this, ((WithMiddleware) servlet).getMiddlewareServlet());
		} else if (method == null && rootServlet == null) {
			rootServlet = servlet;
		} else if (rootServlets.get(method) == null) {
			rootServlets.put(method, servlet);
		} else if (rootServlets.get(method) != servlet) {
			throw new IllegalArgumentException("Can't map. Servlet already exists");
		}
	}

	private void merge(MiddlewareServlet mServlet1, MiddlewareServlet mServlet2) {
		if (mServlet1 == mServlet2) {
			return;
		}

		for (Entry<String, MiddlewareServlet> entry : mServlet2.routes.entrySet()) {
			String key = entry.getKey();
			MiddlewareServlet ms1 = mServlet1.routes.get(key);
			MiddlewareServlet ms2 = entry.getValue();
			if (ms1 != null) {
				merge(ms1, ms2);
			} else {
				mServlet1.routes.put(key, ms2);
			}
		}

		for (Entry<String, MiddlewareServlet> entry : mServlet2.parameters.entrySet()) {
			String name = entry.getKey();
			MiddlewareServlet ps1 = mServlet1.parameters.get(name);
			MiddlewareServlet ps2 = entry.getValue();
			if (ps1 != null) {
				merge(ps1, ps2);
			} else {
				mServlet1.parameters.put(name, ps2);
			}
		}

		for (Entry<HttpMethod, AsyncServlet> entry : mServlet2.rootServlets.entrySet()) {
			HttpMethod key = entry.getKey();
			AsyncServlet s1 = mServlet1.rootServlets.get(key);
			AsyncServlet s2 = entry.getValue();
			if (s1 == null) {
				mServlet1.rootServlets.put(key, s2);
			} else if (s1 != s2) {
				throw new IllegalArgumentException("Can't map. Servlet for this method already exists");
			}
		}

		if (mServlet1.rootServlet == null) {
			mServlet1.rootServlet = mServlet2.rootServlet;
		} else if (mServlet2.rootServlet != null && mServlet1.rootServlet != mServlet2.rootServlet) {
			throw new IllegalArgumentException("Can't map. Servlet for this method already exists");
		}

		if (mServlet1.fallbackServlet == null) {
			mServlet1.fallbackServlet = mServlet2.fallbackServlet;
		} else if (mServlet2.fallbackServlet != null && mServlet1.fallbackServlet != mServlet2.fallbackServlet) {
			throw new IllegalArgumentException("Can't map. Fallback already exists.");
		}

	}

	private MiddlewareServlet ensureMServlet(String urlPart) {
		if (urlPart.startsWith(":")) {
			return parameters.computeIfAbsent(urlPart.substring(1), $ -> create());
		}
		return routes.computeIfAbsent(urlPart, $ -> create());
	}
}
