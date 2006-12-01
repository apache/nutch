<?xml version="1.0"?>
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
-->
<!-- XSLT stylesheet that adds Nutch style, header, and footer
  elements.  This is used by Ant to generate static html pages. -->
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">
  <xsl:output method="html" doctype-public="-//W3C//DTD HTML 4.01 Transitional//EN"/>
  <xsl:template match="page">
      <xsl:comment>This page is automatically generated.  Do not edit!</xsl:comment>
        <table width="635" border="0" cellpadding="0" cellspacing="0">
          <tr valign="top">
            <td width="140">
              <xsl:call-template name="subnavi"/>
            </td>
            <td width="20" background="img/reiter/_spacer_cccccc.gif">
              <xsl:text disable-output-escaping="yes">&amp;#160;</xsl:text>
            </td>
            <td width="475" class="body">
              <xsl:call-template name="body"/>
            </td>
          </tr>
        </table>
  </xsl:template>
<!-- included menu -->
  <xsl:template name="subnavi">
    <table width="100%" cellpadding="0" cellspacing="0">
      <xsl:for-each select="menu/item">
        <xsl:if test="not(.='')">
          <tr class="menuTd" height="25">
            <td class="menuTd" onmouseover="this.className='menuTdhover';" onmouseout="this.className='menuTd'" width="100%">
              <xsl:text disable-output-escaping="yes">&amp;#160;:: </xsl:text>
              <xsl:variable name="url" select="a/@href"/>
              <a href="{$url}" class="menuEntry">
                <xsl:value-of select="."/>
              </a>
            </td>
          </tr>
          <tr height="1px">
            <td>
              <img src="img/reiter/spacer_666666.gif" height="1" width="100%"/>
            </td>
          </tr>
        </xsl:if>
      </xsl:for-each>
      <tr>
        <td>
          <xsl:text disable-output-escaping="yes">&amp;#160;</xsl:text>
        </td>
      </tr>
    </table>
  </xsl:template>
<!-- /included menu -->
<!-- included body -->
  <xsl:template name="body">
    <table width="475" border="0" cellpadding="0" cellspacing="0">
      <tr>
        <td class="title" height="125" width="275" valign="bottom">
          <xsl:value-of select="title" disable-output-escaping="yes"/>
        </td>
        <td height="125" width="200" valign="bottom">
          <img src="img/reiter/robots.gif"/>
        </td>
      </tr>
    </table>
    <br class="br"/>
    <xsl:for-each select="body/node()">
      <xsl:choose>
<!-- orange intro -->
        <xsl:when test="name()='p' and position() &lt; 3">
          <span class="intro">
            <xsl:copy-of select="."/>
          </span>
        </xsl:when>
<!-- all other text -->
        <xsl:otherwise>
          <span class="bodytext">
            <xsl:copy-of select="."/>
          </span>
        </xsl:otherwise>
      </xsl:choose>
    </xsl:for-each>
    <br class="br"/>
    <br class="br"/>
  </xsl:template>
<!-- /included body -->
</xsl:stylesheet>
