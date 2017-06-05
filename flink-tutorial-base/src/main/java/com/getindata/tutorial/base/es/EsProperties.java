/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.getindata.tutorial.base.es;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//TODO set appropriate parameters
public class EsProperties {

	public static Map<String, String> getEsProperties() {
		Map<String, String> config = new HashMap<>();

		config.put("cluster.name", "my-cluster-name");
		// This instructs the sink to emit after every element, otherwise they would be buffered
		config.put("bulk.flush.max.actions", "1");
		return config;
	}

	public static List<InetSocketAddress> getEsAddresses() throws UnknownHostException {
		List<InetSocketAddress> transportAddresses = new ArrayList<>();
		transportAddresses.add(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 9300));
		transportAddresses.add(new InetSocketAddress(InetAddress.getByName("10.2.3.1"), 9300));

		return transportAddresses;
	}

	private EsProperties() {
	}
}
