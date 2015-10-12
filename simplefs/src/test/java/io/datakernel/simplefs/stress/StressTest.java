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

package io.datakernel.simplefs.stress;

import java.io.IOException;

public class StressTest {
	// first should start server form StressServer, then call main as many times as you like
	public static void main(String[] args) {
		try {
			StressClient client = new StressClient();
			client.setup();
			client.start(100, 360_000);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}