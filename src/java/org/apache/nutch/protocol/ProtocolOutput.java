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

package org.apache.nutch.protocol;

/**
 * Simple aggregate to pass from protocol plugins both content and
 * protocol status.
 * @author Andrzej Bialecki &lt;ab@getopt.org&gt;
 */
public class ProtocolOutput {
  private Content content;
  private ProtocolStatus status;

  public ProtocolOutput(Content content, ProtocolStatus status) {
    this.content = content;
    this.status = status;
  }
  
  public ProtocolOutput(Content content) {
    this.content = content;
    this.status = ProtocolStatus.STATUS_SUCCESS;
  }
  
  public Content getContent() {
    return content;
  }

  public void setContent(Content content) {
    this.content = content;
  }

  public ProtocolStatus getStatus() {
    return status;
  }

  public void setStatus(ProtocolStatus status) {
    this.status = status;
  }
}
