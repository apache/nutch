<?xml version="1.0" encoding="ISO-8859-1"?>
<!--
	Licensed to the Apache Software Foundation (ASF) under one or more
	contributor license agreements.  See the NOTICE file distributed with
	this work for additional information regarding copyright ownership.
	The ASF licenses this file to You under the Apache License, Version 2.0
	(the "License"); you may not use this file except in compliance with
	the License.  You may obtain a copy of the License at
	
	http://www.apache.org/licenses/LICENSE-2.0
	
	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.
	
	Author     : Chris Mattmann
        Auhtor     : Jérôme Charron
	Description: This xsl file is used to transform dynamic properties out of the
	nutch-default.xml file into a deployable Context.xml file for configuring
	the Nutch war file in a servlet container.
-->

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

<xsl:template match="/">
<Context path="/nutch" docBase="nutch-0.9-dev.war"
        debug="5" reloadable="true" crossContext="true">

<xsl:for-each select="configuration/property">

<!-- "searcher." properties -->
<xsl:call-template name="parameter">
  <xsl:with-param name="property" select="current()"/>
  <xsl:with-param name="filter" select="'searcher.'"/>
</xsl:call-template>

<!-- "plugin." properties -->
<xsl:call-template name="parameter">
  <xsl:with-param name="property" select="current()"/>
  <xsl:with-param name="filter" select="'plugin.'"/>
</xsl:call-template>

<!-- "extension.clustering." properties -->
<xsl:call-template name="parameter">
  <xsl:with-param name="property" select="current()"/>
  <xsl:with-param name="filter" select="'extension.clustering.'"/>
</xsl:call-template>

<!-- "extension.ontology." properties -->
<xsl:call-template name="parameter">
  <xsl:with-param name="property" select="current()"/>
  <xsl:with-param name="filter" select="'extension.ontology.'"/>
</xsl:call-template>

<!-- "query." properties -->
<xsl:call-template name="parameter">
  <xsl:with-param name="property" select="current()"/>
  <xsl:with-param name="filter" select="'query.'"/>
</xsl:call-template>

</xsl:for-each>

</Context>
</xsl:template>


<!--
 ! Template used to write out a parameter if the property's
 ! name contains the specified filter string.
 !-->
<xsl:template name="parameter">
  <xsl:param name="property"/>
  <xsl:param name="filter"/>
  <xsl:if test="contains(name, $filter)">
  <Parameter override="false">
    <xsl:attribute name="name">
      <xsl:value-of select="name"/>
    </xsl:attribute>
    <xsl:attribute name="value">
      <xsl:value-of select="value"/>
    </xsl:attribute>
  </Parameter>
  </xsl:if>
</xsl:template>

</xsl:stylesheet> 
