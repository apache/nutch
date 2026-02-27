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
package org.apache.nutch.indexer.geoip;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.InetAddress;
import java.util.Optional;

import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.indexer.NutchField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.WebServiceClient;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.AnonymousIpResponse;
import com.maxmind.geoip2.model.AsnResponse;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.model.ConnectionTypeResponse;
import com.maxmind.geoip2.model.CountryResponse;
import com.maxmind.geoip2.model.DomainResponse;
import com.maxmind.geoip2.model.InsightsResponse;
import com.maxmind.geoip2.model.IspResponse;
import com.maxmind.geoip2.record.City;
import com.maxmind.geoip2.record.Continent;
import com.maxmind.geoip2.record.Country;
import com.maxmind.geoip2.record.Location;
import com.maxmind.geoip2.record.Postal;
import com.maxmind.geoip2.record.RepresentedCountry;
import com.maxmind.geoip2.record.Subdivision;
import com.maxmind.geoip2.record.Anonymizer;
import com.maxmind.geoip2.record.Traits;

/**
 * <p>
 * Simple utility class which builds a
 * {@link org.apache.nutch.indexer.NutchDocument} based on input from
 * {@link GeoIPIndexingFilter}, where configuration is also read.
 * </p>
 */
public class GeoIPDocumentCreator {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  // Database-specific network address field names
  static final String ANONYMOUS_NETWORK_ADDRESS = "anonymousNetworkAddress";
  static final String ASN_NETWORK_ADDRESS = "asnNetworkAddress";
  static final String CITY_NETWORK_ADDRESS = "cityNetworkAddress";
  static final String CONNECTION_NETWORK_ADDRESS = "connectionNetworkAddress";
  static final String COUNTRY_NETWORK_ADDRESS = "countryNetworkAddress";
  static final String DOMAIN_NETWORK_ADDRESS = "domainNetworkAddress";
  static final String INSIGHTS_NETWORK_ADDRESS = "insightsNetworkAddress";

  private GeoIPDocumentCreator() {}

  /**
   * Add field to document but only if value isn't null
   * @param doc the {@link NutchDocument} to augment
   * @param name the name of the target field
   * @param value the String value to associate with the target field
   */
  private static void addIfNotNull(NutchDocument doc, String name,
      Object value) {
    if (value != null) {
      doc.add(name, value);
    }
  }

  /**
   * Add field to document only if the field doesn't already contain this exact value.
   * Use for fields like "ip" that should not have duplicate values.
   * @param doc the {@link NutchDocument} to augment
   * @param name the name of the target field
   * @param value the value to associate with the target field
   */
  static void addIfNotDuplicate(NutchDocument doc, String name, Object value) {
    if (value == null) {
      return;
    }
    NutchField field = doc.getField(name);
    if (field == null || !field.getValues().contains(value)) {
      doc.add(name, value);
    }
  }

  /**
   * Populate a {@link org.apache.nutch.indexer.NutchDocument} based on lookup
   * of IP in Anonymous IP database.
   * @param serverIp the server IP address to lookup
   * @param doc NutchDocument to populate
   * @param reader instantiated DatabaseReader object
   * @return populated NutchDocument
   * @throws IOException if an error occurs performing the Db lookup
   * @throws GeoIp2Exception generic GeoIp2 exception
   */
  public static NutchDocument createDocFromAnonymousIpDb(String serverIp,
      NutchDocument doc, DatabaseReader reader) throws IOException, GeoIp2Exception {
    Optional<AnonymousIpResponse> opt = reader.tryAnonymousIp(InetAddress.getByName(serverIp));
    if (opt.isPresent()) {
      AnonymousIpResponse response = opt.get();
      addIfNotDuplicate(doc, "ip", response.ipAddress());
      addIfNotNull(doc, ANONYMOUS_NETWORK_ADDRESS, response.network().toString());
      addIfNotNull(doc, "isAnonymous", response.isAnonymous());
      addIfNotNull(doc, "isAnonymousVpn", response.isAnonymousVpn());
      addIfNotNull(doc, "isHostingProxy", response.isHostingProvider());
      addIfNotNull(doc, "isPublicProxy", response.isPublicProxy());
      addIfNotNull(doc, "isResidentialProxy", response.isResidentialProxy());
      addIfNotNull(doc, "isTorExitNode", response.isTorExitNode());
    } else {
      LOG.debug("'{}' IP address not found in Anonymous IP DB.", serverIp);
    }
    return doc;
  }

  /**
   * Populate a {@link org.apache.nutch.indexer.NutchDocument} based on lookup
   * of IP in ASN database.
   * @param serverIp the server IP address to lookup
   * @param doc NutchDocument to populate
   * @param reader instantiated DatabaseReader object
   * @return populated NutchDocument
   * @throws IOException if an error occurs performing the Db lookup
   * @throws GeoIp2Exception generic GeoIp2 exception
   */
  public static NutchDocument createDocFromAsnDb(String serverIp,
      NutchDocument doc, DatabaseReader reader) throws IOException, GeoIp2Exception {
    Optional<AsnResponse> opt = reader.tryAsn(InetAddress.getByName(serverIp));
    if (opt.isPresent()) {
      AsnResponse response = opt.get();
      addIfNotDuplicate(doc, "ip", response.ipAddress());
      addIfNotNull(doc, ASN_NETWORK_ADDRESS, response.network().toString());
      addIfNotNull(doc, "autonomousSystemNumber", response.autonomousSystemNumber());
      addIfNotNull(doc, "autonomousSystemOrganization", response.autonomousSystemOrganization());
    } else {
      LOG.debug("'{}' IP address not found in ASN DB.", serverIp);
    }
    return doc;
  }

  /**
   * Populate a {@link org.apache.nutch.indexer.NutchDocument} based on lookup
   * of IP in City database.
   * @param serverIp the server IP address to lookup
   * @param doc NutchDocument to populate
   * @param reader instantiated DatabaseReader object
   * @return populated NutchDocument
   * @throws IOException if an error occurs performing the Db lookup
   * @throws GeoIp2Exception generic GeoIp2 exception
   */
  public static NutchDocument createDocFromCityDb(String serverIp,
      NutchDocument doc, DatabaseReader reader) throws IOException, GeoIp2Exception {
    addIfNotDuplicate(doc, "ip", serverIp);
    Optional<CityResponse> opt = reader.tryCity(InetAddress.getByName(serverIp));
    if (opt.isPresent()) {
      processCityDocument(doc, opt.get());
    } else {
      LOG.debug("'{}' IP address not found in City DB.", serverIp);
    }
    return doc;
  }

  private static NutchDocument processCityDocument(NutchDocument doc, CityResponse response) {
    City city = response.city();
    addIfNotNull(doc, "cityName", city.name());
    addIfNotNull(doc, "cityConfidence", city.confidence());
    addIfNotNull(doc, "cityGeoNameId", city.geonameId());

    Continent continent = response.continent();
    addIfNotNull(doc, "continentCode", continent.code());
    addIfNotNull(doc, "continentGeoNameId", continent.geonameId());
    addIfNotNull(doc, "continentName", continent.name());

    Country country = response.registeredCountry();
    addIfNotNull(doc, "countryIsoCode", country.isoCode());
    addIfNotNull(doc, "countryName", country.name());
    addIfNotNull(doc, "countryConfidence", country.confidence());
    addIfNotNull(doc, "countryGeoNameId", country.geonameId());
    addIfNotNull(doc, "countryInEuropeanUnion", country.isInEuropeanUnion());

    Location location = response.location();
    if (location.latitude() != null && location.longitude() != null) {
      addIfNotNull(doc, "latLon", location.latitude() + "," + location.longitude());
    }
    addIfNotNull(doc, "accuracyRadius", location.accuracyRadius());
    addIfNotNull(doc, "timeZone", location.timeZone());
    addIfNotNull(doc, "populationDensity", location.populationDensity());

    Postal postal = response.postal();
    addIfNotNull(doc, "postalCode", postal.code());
    addIfNotNull(doc, "postalConfidence", postal.confidence());

    RepresentedCountry rCountry = response.representedCountry();
    addIfNotNull(doc, "countryType", rCountry.type());

    Subdivision mostSubdivision = response.mostSpecificSubdivision();
    addIfNotNull(doc, "mostSpecificSubDivName", mostSubdivision.name());
    addIfNotNull(doc, "mostSpecificSubDivIsoCode", mostSubdivision.isoCode());
    addIfNotNull(doc, "mostSpecificSubDivConfidence", mostSubdivision.confidence());
    addIfNotNull(doc, "mostSpecificSubDivGeoNameId", mostSubdivision.geonameId());

    Subdivision leastSubdivision = response.leastSpecificSubdivision();
    addIfNotNull(doc, "leastSpecificSubDivName", leastSubdivision.name());
    addIfNotNull(doc, "leastSpecificSubDivIsoCode", leastSubdivision.isoCode());
    addIfNotNull(doc, "leastSpecificSubDivConfidence", leastSubdivision.confidence());
    addIfNotNull(doc, "leastSpecificSubDivGeoNameId", leastSubdivision.geonameId());

    Traits traits = response.traits();
    addIfNotNull(doc, "autonomousSystemNumber", traits.autonomousSystemNumber());
    addIfNotNull(doc, "autonomousSystemOrganization", traits.autonomousSystemOrganization());
    if (traits.connectionType() != null) {
      addIfNotNull(doc, "connectionType", traits.connectionType().toString());
    }
    addIfNotNull(doc, "domain", traits.domain());
    addIfNotNull(doc, "isp", traits.isp());
    addIfNotNull(doc, "mobileCountryCode", traits.mobileCountryCode());
    addIfNotNull(doc, "mobileNetworkCode", traits.mobileNetworkCode());
    if (traits.network() != null) {
      addIfNotNull(doc, CITY_NETWORK_ADDRESS, traits.network().toString());
    }
    addIfNotNull(doc, "organization", traits.organization());
    addIfNotNull(doc, "staticIpScore", traits.staticIpScore());
    addIfNotNull(doc, "userCount", traits.userCount());
    addIfNotNull(doc, "userType", traits.userType());
    addIfNotNull(doc, "isAnycast", traits.isAnycast());
    addIfNotNull(doc, "isLegitimateProxy", traits.isLegitimateProxy());
    return doc;
  }

  /**
   * Populate a {@link org.apache.nutch.indexer.NutchDocument} based on lookup
   * of IP in ConnectionDb.
   * @param serverIp the server IP
   * @param doc NutchDocument to populate
   * @param reader instantiated DatabaseReader object
   * @return populated NutchDocument
   * @throws IOException if an error occurs performing the Db lookup
   * @throws GeoIp2Exception generic GeoIp2 exception
   */
  public static NutchDocument createDocFromConnectionDb(String serverIp,
      NutchDocument doc, DatabaseReader reader) throws IOException, GeoIp2Exception {
    Optional<ConnectionTypeResponse> opt = reader.tryConnectionType(InetAddress
        .getByName(serverIp));
    if (opt.isPresent()) {
      ConnectionTypeResponse response = opt.get();
      addIfNotDuplicate(doc, "ip", response.ipAddress());
      if (response.connectionType() != null) {
        addIfNotNull(doc, "connectionType", response.connectionType().toString());
      }
      if (response.network() != null) {
        addIfNotNull(doc, CONNECTION_NETWORK_ADDRESS, response.network().toString());
      }
    } else {
      LOG.debug("'{}' IP address not found in Connection DB.", serverIp);
    }
    return doc;
  }

  /**
   * Populate a {@link org.apache.nutch.indexer.NutchDocument} based on lookup
   * of IP in Country database. This is a lighter-weight alternative to the City
   * database when only country-level information is needed.
   * @param serverIp the server IP address to lookup
   * @param doc NutchDocument to populate
   * @param reader instantiated DatabaseReader object
   * @return populated NutchDocument
   * @throws IOException if an error occurs performing the Db lookup
   * @throws GeoIp2Exception generic GeoIp2 exception
   */
  public static NutchDocument createDocFromCountryDb(String serverIp,
      NutchDocument doc, DatabaseReader reader) throws IOException, GeoIp2Exception {
    Optional<CountryResponse> opt = reader.tryCountry(InetAddress.getByName(serverIp));
    if (opt.isPresent()) {
      CountryResponse response = opt.get();
      addIfNotDuplicate(doc, "ip", serverIp);

      Continent continent = response.continent();
      addIfNotDuplicate(doc, "continentCode", continent.code());
      addIfNotDuplicate(doc, "continentGeoNameId", continent.geonameId());
      addIfNotDuplicate(doc, "continentName", continent.name());

      Country country = response.registeredCountry();
      addIfNotDuplicate(doc, "countryIsoCode", country.isoCode());
      addIfNotDuplicate(doc, "countryName", country.name());
      addIfNotDuplicate(doc, "countryConfidence", country.confidence());
      addIfNotDuplicate(doc, "countryGeoNameId", country.geonameId());
      addIfNotDuplicate(doc, "countryInEuropeanUnion", country.isInEuropeanUnion());

      RepresentedCountry rCountry = response.representedCountry();
      addIfNotDuplicate(doc, "countryType", rCountry.type());

      Traits traits = response.traits();
      if (traits.network() != null) {
        addIfNotNull(doc, COUNTRY_NETWORK_ADDRESS, traits.network().toString());
      }
      addIfNotDuplicate(doc, "isAnycast", traits.isAnycast());
      addIfNotDuplicate(doc, "isLegitimateProxy", traits.isLegitimateProxy());
    } else {
      LOG.debug("'{}' IP address not found in Country DB.", serverIp);
    }
    return doc;
  }

  /**
   * Populate a {@link org.apache.nutch.indexer.NutchDocument} based on lookup
   * of IP in Domain database.
   * @param serverIp the server IP address to lookup
   * @param doc NutchDocument to populate
   * @param reader instantiated DatabaseReader object
   * @return populated NutchDocument
   * @throws IOException if an error occurs performing the Db lookup
   * @throws GeoIp2Exception generic GeoIp2 exception
   */
  public static NutchDocument createDocFromDomainDb(String serverIp,
      NutchDocument doc, DatabaseReader reader) throws IOException, GeoIp2Exception {
    Optional<DomainResponse> opt = reader.tryDomain(InetAddress.getByName(serverIp));
    if (opt.isPresent()) {
      DomainResponse response = opt.get();
      addIfNotDuplicate(doc, "ip", response.ipAddress());
      addIfNotNull(doc, "domain", response.domain());
      addIfNotNull(doc, DOMAIN_NETWORK_ADDRESS, response.network().toString());
    } else {
      LOG.debug("'{}' IP address not found in Domain DB.", serverIp);
    }
    return doc;
  }

  /**
   * Populate a {@link org.apache.nutch.indexer.NutchDocument} based on lookup
   * of IP using the MaxMind Insights web service.
   * @param serverIp the server IP address to lookup
   * @param doc NutchDocument to populate
   * @param client WebServiceClient for MaxMind Insights API
   * @return populated NutchDocument
   * @throws IOException if an error occurs performing the lookup
   * @throws GeoIp2Exception generic GeoIp2 exception
   */
  public static NutchDocument createDocFromInsightsService(String serverIp,
      NutchDocument doc, WebServiceClient client) throws IOException, GeoIp2Exception {
    addIfNotDuplicate(doc, "ip", serverIp);
    return processInsightsDocument(doc, client.insights(InetAddress.getByName(serverIp)));
  }

  private static NutchDocument processInsightsDocument(NutchDocument doc, InsightsResponse response) {
    City city = response.city();
    addIfNotNull(doc, "cityName", city.name());
    addIfNotNull(doc, "cityConfidence", city.confidence());
    addIfNotNull(doc, "cityGeoNameId", city.geonameId());

    Continent continent = response.continent();
    addIfNotNull(doc, "continentCode", continent.code());
    addIfNotNull(doc, "continentGeoNameId", continent.geonameId());
    addIfNotNull(doc, "continentName", continent.name());

    Country country = response.registeredCountry();
    addIfNotNull(doc, "countryIsoCode", country.isoCode());
    addIfNotNull(doc, "countryName", country.name());
    addIfNotNull(doc, "countryConfidence", country.confidence());
    addIfNotNull(doc, "countryGeoNameId", country.geonameId());
    addIfNotNull(doc, "countryInEuropeanUnion", country.isInEuropeanUnion());

    Location location = response.location();
    if (location.latitude() != null && location.longitude() != null) {
      addIfNotNull(doc, "latLon", location.latitude() + "," + location.longitude());
    }
    addIfNotNull(doc, "accuracyRadius", location.accuracyRadius());
    addIfNotNull(doc, "timeZone", location.timeZone());
    addIfNotNull(doc, "populationDensity", location.populationDensity());

    Postal postal = response.postal();
    addIfNotNull(doc, "postalCode", postal.code());
    addIfNotNull(doc, "postalConfidence", postal.confidence());

    RepresentedCountry rCountry = response.representedCountry();
    addIfNotNull(doc, "countryType", rCountry.type());

    Subdivision mostSubdivision = response.mostSpecificSubdivision();
    addIfNotNull(doc, "mostSpecificSubDivName", mostSubdivision.name());
    addIfNotNull(doc, "mostSpecificSubDivIsoCode", mostSubdivision.isoCode());
    addIfNotNull(doc, "mostSpecificSubDivConfidence", mostSubdivision.confidence());
    addIfNotNull(doc, "mostSpecificSubDivGeoNameId", mostSubdivision.geonameId());

    Subdivision leastSubdivision = response.leastSpecificSubdivision();
    addIfNotNull(doc, "leastSpecificSubDivName", leastSubdivision.name());
    addIfNotNull(doc, "leastSpecificSubDivIsoCode", leastSubdivision.isoCode());
    addIfNotNull(doc, "leastSpecificSubDivConfidence", leastSubdivision.confidence());
    addIfNotNull(doc, "leastSpecificSubDivGeoNameId", leastSubdivision.geonameId());

    Traits traits = response.traits();
    addIfNotNull(doc, "autonomousSystemNumber", traits.autonomousSystemNumber());
    addIfNotNull(doc, "autonomousSystemOrganization", traits.autonomousSystemOrganization());
    if (traits.connectionType() != null) {
      addIfNotNull(doc, "connectionType", traits.connectionType().toString());
    }
    addIfNotNull(doc, "domain", traits.domain());
    addIfNotNull(doc, "isp", traits.isp());
    addIfNotNull(doc, "mobileCountryCode", traits.mobileCountryCode());
    addIfNotNull(doc, "mobileNetworkCode", traits.mobileNetworkCode());
    if (traits.network() != null) {
      addIfNotNull(doc, INSIGHTS_NETWORK_ADDRESS, traits.network().toString());
    }
    addIfNotNull(doc, "organization", traits.organization());
    addIfNotNull(doc, "staticIpScore", traits.staticIpScore());
    addIfNotNull(doc, "userCount", traits.userCount());
    addIfNotNull(doc, "userType", traits.userType());
    addIfNotNull(doc, "isAnycast", traits.isAnycast());
    addIfNotNull(doc, "isLegitimateProxy", traits.isLegitimateProxy());
    Anonymizer anonymizer = response.anonymizer();
    addIfNotNull(doc, "isAnonymous", anonymizer.isAnonymous());
    addIfNotNull(doc, "isAnonymousVpn", anonymizer.isAnonymousVpn());
    addIfNotNull(doc, "isHostingProvider", anonymizer.isHostingProvider());
    addIfNotNull(doc, "isPublicProxy", anonymizer.isPublicProxy());
    addIfNotNull(doc, "isResidentialProxy", anonymizer.isResidentialProxy());
    addIfNotNull(doc, "isTorExitNode", anonymizer.isTorExitNode());
    return doc;
  }

  /**
   * Populate a {@link org.apache.nutch.indexer.NutchDocument} based on lookup
   * of IP in ISP database.
   * @param serverIp the server IP address to lookup
   * @param doc NutchDocument to populate
   * @param reader instantiated DatabaseReader object
   * @return populated NutchDocument
   * @throws IOException if an error occurs performing the Db lookup
   * @throws GeoIp2Exception generic GeoIp2 exception
   */
  public static NutchDocument createDocFromIspDb(String serverIp,
      NutchDocument doc, DatabaseReader reader) throws IOException, GeoIp2Exception {
    Optional<IspResponse> opt = reader.tryIsp(InetAddress.getByName(serverIp));
    if (opt.isPresent()) {
      IspResponse response = opt.get();
      addIfNotDuplicate(doc, "ip", response.ipAddress());
      addIfNotNull(doc, "autonSystemNum", response.autonomousSystemNumber());
      addIfNotNull(doc, "autonSystemOrg", response.autonomousSystemOrganization());
      addIfNotNull(doc, "isp", response.isp());
      addIfNotNull(doc, "org", response.organization());
    } else {
      LOG.debug("'{}' IP address not found in ISP DB.", serverIp);
    }
    return doc;
  }

}
