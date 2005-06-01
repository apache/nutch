/* Copyright (c) 2003 The Nutch Organization.  All rights reserved.   */
/* Use subject to the conditions in http://www.nutch.org/LICENSE.txt. */

package org.apache.nutch.protocol.httpclient;

/**
 * Can be used to identify problems during creation of Authentication objects.
 * In the future it may be used as a method of collecting authentication
 * failures during Http protocol transfer in order to present the user with
 * credentials required during a future fetch.
 * 
 * @author Matt Tencati
 */
public class HttpAuthenticationException extends Exception {

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
