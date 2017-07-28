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

package io.datakernel.dns;

import io.datakernel.dns.DnsMessage.ResponseErrorCode;

import java.net.InetAddress;

final class CachedDnsLookupResult {
	private final String domainName;
	private final InetAddress[] ips;
	private final long expirationMillisecond;
	private final ResponseErrorCode errorCode;
	private final Short type;

	private CachedDnsLookupResult(String domainName, InetAddress[] ips, Long expirationMillisecond,
	                              ResponseErrorCode errorCode, Short type) {
		this.domainName = domainName;
		this.ips = ips;
		this.expirationMillisecond = expirationMillisecond;
		this.errorCode = errorCode;
		this.type = type;
	}

	/**
	 * Creates a new CachedDnsLookupResult from DnsQueryResult
	 *
	 * @param queryResult           result which was received from DNS server
	 * @param expirationMillisecond time to live for this record
	 * @return new CachedDnsLookupResult
	 */
	public static CachedDnsLookupResult fromQueryWithExpiration(DnsQueryResult queryResult, long expirationMillisecond) {
		return new CachedDnsLookupResult(queryResult.getDomainName(), queryResult.getIps(), expirationMillisecond,
				queryResult.getErrorCode(), queryResult.getType());
	}

	/**
	 * Creates a new CachedDnsLookupResult from DnsException
	 *
	 * @param exception             exception which was received from DNS server
	 * @param expirationMillisecond time to live for this record
	 * @return new CachedDnsLookupResult
	 */
	public static CachedDnsLookupResult fromExceptionWithExpiration(DnsException exception, long expirationMillisecond) {
		return new CachedDnsLookupResult(exception.getDomainName(), null, expirationMillisecond, exception.getErrorCode(),
				null);
	}

	public DnsException getException() {
		return !isSuccessful() ? new DnsException(domainName, errorCode) : null;
	}

	public String getDomainName() {
		return domainName;
	}

	public InetAddress[] getIps() {
		return ips;
	}

	public ResponseErrorCode getErrorCode() {
		return errorCode;
	}

	public boolean isSuccessful() {
		return errorCode == ResponseErrorCode.NO_ERROR;
	}

	public long getExpirationMillisecond() {
		return expirationMillisecond;
	}

	public Short getType() {
		return type;
	}
}
