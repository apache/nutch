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
package org.apache.nutch.webui.view;

import static de.agilecoders.wicket.core.markup.html.bootstrap.block.LabelType.Success;
import static org.apache.nutch.webui.client.model.ConnectionStatus.CONNECTED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.nutch.webui.client.model.ConnectionStatus;
import org.apache.nutch.webui.pages.components.ColorEnumLabel;
import org.apache.nutch.webui.pages.components.ColorEnumLabelBuilder;
import org.apache.wicket.model.Model;
import org.junit.Test;

public class TestColorEnumLabel extends AbstractWicketTest {

  @Test
  public void shouldRenderCorrectText() {
    // given
    Model<ConnectionStatus> model = Model.of(ConnectionStatus.CONNECTED);
    ColorEnumLabel<ConnectionStatus> label = new ColorEnumLabelBuilder<ConnectionStatus>("status")
        .withModel(model).build();

    // when
    tester.startComponentInPage(label);

    // then
    assertEquals("Connected", tester.getTagByWicketId("status").getValue());
  }

  @Test
  public void shouldChangeColorOfLabel() {
    // given
    Model<ConnectionStatus> model = Model.of(ConnectionStatus.CONNECTED);
    ColorEnumLabel<ConnectionStatus> label = new ColorEnumLabelBuilder<ConnectionStatus>("status")
        .withEnumColor(CONNECTED, Success).withModel(model).build();

    // when
    tester.startComponentInPage(label);

    // then
    assertTrue(tester.getTagByWicketId("status").getAttributeEndsWith("class", "success"));
  }
}
