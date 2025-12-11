/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Metrics infrastructure for Apache Nutch.
 * 
 * <p>This package provides centralized constants and utilities for Hadoop
 * MapReduce metrics/counters following
 * <a href="https://prometheus.io/docs/practices/naming/">Prometheus naming
 * conventions</a>.
 * 
 * <p>The main class is {@link org.apache.nutch.metrics.NutchMetrics} which
 * defines all counter group names and counter names as constants.
 * 
 * @since 1.22
 */
package org.apache.nutch.metrics;

