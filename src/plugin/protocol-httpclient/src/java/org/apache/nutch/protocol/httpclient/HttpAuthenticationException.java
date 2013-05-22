/**
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
package org.apache.nutch.protocol.httpclient;

/**
 * Can be used to identify problems during creation of Authentication objects.
 * In the future it may be used as a method of collecting authentication
 * failures during Http protocol transfer in order to present the user with
 * credentials required during a future fetch.
 */
public class HttpAuthenticationException extends Exception {

  private static final long serialVersionUID = 1L;

    /**
     *  Constructs a new exception with null as its detail message.
     */
    public HttpAuthenticationException() {
        super();
    }

    /**
     * Constructs a new exception with the specified detail message.
     * 
     * @param message the detail message. The detail message is saved for later retrieval by the {@link Throwable#getMessage()} method.
     */
    public HttpAuthenticationException(String message) {
        super(message);
    }

    /**
     * Constructs a new exception with the specified message and cause.
     *
     * @param message the detail message. The detail message is saved for later retrieval by the {@link Throwable#getMessage()} method.
     * @param cause the cause (use {@link #getCause()} to retrieve the cause)
     */
    public HttpAuthenticationException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs a new exception with the specified cause and detail message from
     * given clause if it is not null.
     * 
     * @param cause the cause (use {@link #getCause()} to retrieve the cause)
     */
    public HttpAuthenticationException(Throwable cause) {
        super(cause);
    }

}
