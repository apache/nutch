<?xml version="1.0"?>
<!-- XSLT stylesheet that adds Nutch style, header, and footer
  elements.  This is used by Ant to generate static html pages. -->
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">
  <xsl:output method="html" doctype-public="-//W3C//DTD HTML 4.01 Transitional//EN"/>
  <xsl:template match="page">
    <html>
      <xsl:comment>This page is automatically generated.  Do not edit!</xsl:comment>
      <head>
<!-- page title -->
        <title>
          <xsl:text>Nutch: </xsl:text>
          <xsl:value-of select="title" disable-output-escaping="yes"/>
        </title>
<!-- insert style -->
        <xsl:copy-of select="document('../include/style.html')"/>
<!-- specify icon file -->
      <link rel="icon" href="../img/favicon.ico" type="image/x-icon"/>
      <link rel="shortcut icon" href="../img/favicon.ico" type="image/x-icon"/>

      <script type="text/javascript">
      <xsl:comment>
function queryfocus() {
  search = document.search;
  if (search != null) { search.query.focus(); }
}
<xsl:text>// </xsl:text>
</xsl:comment>
      </script>
      </head>
      <body onLoad="queryfocus();">
<!-- insert localized header -->
        <xsl:copy-of select="document('include/header.html')"/>
        <table width="635" border="0" cellpadding="0" cellspacing="0">
          <tr valign="top">
            <td width="140">
              <xsl:call-template name="subnavi"/>
            </td>
            <td width="20" background="../img/reiter/_spacer_cccccc.gif">
              <xsl:text disable-output-escaping="yes">&amp;#160;</xsl:text>
            </td>
            <td width="475" class="body">
              <xsl:call-template name="body"/>
            </td>
          </tr>
        </table>
<!-- insert nutch footer -->
        <xsl:copy-of select="document('../include/footer.html')"/>
      </body>
    </html>
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
              <img src="../img/reiter/spacer_666666.gif" height="1" width="100%"/>
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
          <img src="../img/reiter/robots.gif"/>
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
