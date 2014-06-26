/*******************************************************************************
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
 ******************************************************************************/
package org.apache.nutch.api.misc;

import org.apache.nutch.api.model.response.ErrorResponse;
import org.restlet.Request;
import org.restlet.Response;
import org.restlet.data.Status;
import org.restlet.ext.jackson.JacksonRepresentation;
import org.restlet.representation.Representation;
import org.restlet.service.StatusService;

public class ErrorStatusService extends StatusService {
  @Override
  public Status getStatus(Throwable throwable, Request request,
      Response response) {
    return new Status(Status.SERVER_ERROR_INTERNAL, throwable);
  }

  @Override
  public Representation getRepresentation(Status status, Request request,
      Response response) {
    ErrorResponse errorResponse = new ErrorResponse(status.getThrowable());
    return new JacksonRepresentation<ErrorResponse>(errorResponse);
  }
}
