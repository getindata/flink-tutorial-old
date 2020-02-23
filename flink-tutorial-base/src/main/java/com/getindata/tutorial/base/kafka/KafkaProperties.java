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

package com.getindata.tutorial.base.kafka;

import java.util.Properties;

public class KafkaProperties {

    public static String getUsername() {
        // FIXME please return your username
        // return "lion";
        throw new UnsupportedOperationException("Please provide your user name.");
    }

    public static Properties getKafkaProperties() {
        final Properties properties = new Properties();
        // FIXME: uncomment if you are going to use docker
        // properties.setProperty("bootstrap.servers", "kafka:9092");
        // return properties;
        // FIXME: uncomment if you are going to use yarn cluster
        // properties.setProperty("bootstrap.servers", "34.73.186.13:9092,35.185.27.125:9092,35.237.64.158:9092,35.243.129.90:9092,34.74.44.136:9092");
        // return properties;
        throw new UnsupportedOperationException("Please provide Kafka bootstrap servers.");
    }

    public static String getTopic(String user) {
        return "songs_" + user;
    }

    public static String getOutputTopic(String user) {
        return "statistics_" + user;
    }

    private KafkaProperties() {
    }
}
