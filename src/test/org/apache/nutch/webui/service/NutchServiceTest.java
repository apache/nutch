package org.apache.nutch.webui.service;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;

import java.util.Map;

import org.apache.commons.collections4.MapUtils;
import org.apache.nutch.webui.client.NutchClientFactory;
import org.apache.nutch.webui.client.impl.NutchClientImpl;
import org.apache.nutch.webui.model.NutchInstance;
import org.apache.nutch.webui.service.impl.NutchServiceImpl;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.Maps;
import com.sun.jersey.api.client.ClientHandlerException;

@RunWith(MockitoJUnitRunner.class)
public class NutchServiceTest {

  @Mock
  private NutchClientFactory clientFactory;

  @Mock
  private NutchInstanceService instanceService;

  @Mock
  private NutchClientImpl client;

  @InjectMocks
  private NutchServiceImpl nutchService;

  @Test
  public void shouldReturnEmptyMapOnException() {
    // given
    given(clientFactory.getClient(any(NutchInstance.class))).willReturn(client);
    given(client.getNutchConfig("default")).willThrow(new ClientHandlerException("Error!"));

    // when
    Map<String, String> config = nutchService.getNutchConfig(1L);

    // then
    assertTrue(MapUtils.isEmpty(config));
  }

  @Test
  public void shouldGetCorrectConfiguration() {
    // given
    Map<String, String> configMap = Maps.newHashMap();
    given(clientFactory.getClient(any(NutchInstance.class))).willReturn(client);
    given(client.getNutchConfig("default")).willReturn(configMap);

    // when
    Map<String, String> config = nutchService.getNutchConfig(1L);

    // then
    assertSame(configMap, config);
  }
}
