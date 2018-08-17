<?xml version="1.0" encoding="UTF-8"?>
<!--
   Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing,
   software distributed under the License is distributed on an
   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   KIND, either express or implied.  See the License for the
   specific language governing permissions and limitations
   under the License.
-->
<!-- XSLT to extract third-party licenses in a tabular view, see ant target `ant report-license` -->
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  <xsl:output method="text"/>
<xsl:template match="/ivy-report">
  <xsl:text>Name&#x09;Organization&#x09;Revision&#x09;PubDate&#x09;Licenses...&#x0a;</xsl:text>
  <xsl:for-each select="dependencies/module">
    <xsl:value-of select="@name"/>
    <xsl:text>&#x09;</xsl:text>
    <xsl:value-of select="@organisation"/>
    <xsl:for-each select="revision[not(@evicted)]/license">
      <xsl:text>&#x09;</xsl:text>
      <xsl:value-of select="../@name"/>
      <xsl:text>&#x09;</xsl:text>
      <xsl:value-of select="../@pubdate"/>
      <xsl:text>&#x09;</xsl:text>
      <xsl:value-of select="@name"/>
      <xsl:text>&#x09;</xsl:text>
      <xsl:value-of select="@url"/>
    </xsl:for-each>
    <xsl:text>&#x0a;</xsl:text>
  </xsl:for-each>
</xsl:template>

</xsl:stylesheet>
