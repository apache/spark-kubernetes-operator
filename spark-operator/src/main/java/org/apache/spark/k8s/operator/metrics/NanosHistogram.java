/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.k8s.operator.metrics;

/**
 * A {@link SummingHistogram} of durations in nanoseconds.
 *
 * <p>With the default text-based format, {@link PrometheusPullModelHandler} exports its values in
 * seconds and keeps its name, unlike a histogram whose name contains {@code nanos}. Unlike a {@link
 * SummingTimer}, it has no {@link com.codahale.metrics.Meter}, so it is exported without the rates
 * of a timer.
 */
public class NanosHistogram extends SummingHistogram {}
