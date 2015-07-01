ParseReplace plugin

Allows post-parsing regexp replace manipulation of metadata fields.

Configuration Example
    <property>
      <name>parse.replace.regexp</name>
      <value>
        id=/file:/http:/
        url=/file:/http:/128
      </value>
    </property

Property format: parse.replace.regexp
    The format of the property is a list of regexp replacements, one line per field being
    modified.  Field names would be one of those from https://wiki.apache.org/nutch/IndexStructure.

    The fieldname preceeds the equal sign.  The first character after the equal sign signifies
    the delimiter for the regexp, the replacement value and the flags.

Replacement Sequence
    The replacements will happen in the order listed. If a field needs multiple replacement operations
    they may be listed more than once.

RegExp Format
    The regexp and the optional flags should correspond to Pattern.compile(String regexp, int flags) defined
    here: http://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html#compile%28java.lang.String,%20int%29
    Patterns are compiled when the plugin is initialized for efficiency.

Replacement Format
    The replacement value should correspond to Java Matcher(CharSequence input).replaceAll(String replacement):
    http://docs.oracle.com/javase/7/docs/api/java/util/regex/Matcher.html#replaceAll%28java.lang.String%29

Flags
    The flags is an integer sum of the flag values defined in
    http://docs.oracle.com/javase/7/docs/api/constant-values.html (Sec: java.util.regex.Pattern)

Escaping
    Since the regexp is being read from a config file, any escaped values must be double
    escaped.  Eg:  id=/\\s+//  will cause the esacped \s+ match pattern to be used.

Multi-valued Fields
    If a field has multiple values, the replacement will be applied to each value in turn.

Non-string Datatypes
    Replacement is possible only on String field datatypes.  If the field you name in the property is
    not a String datatype, it will be silently ignored.

Host and URL specifc replacements.
    If the replacements should apply only to specifc pages, then add a sequence like

    hostmatch=/host match pattern/
    fld1=/regexp/replace/flags
    fld2=/regexp/replace/flags

    or
    urlmatch=/url match pattern/
    fld1=/regexp/replace/flags
    fld2=/regexp/replace/flags

When using Host and URL replacements, all replacements preceding the first hostmatch or urlmatch
will apply to all parsed pages.  Replacements following a hostmatch or urlmatch will be applied
to pages which match the host or url field (up to the next hostmatch or urlmatch line).  hostmatch
and urlmatch patterns must be unique in this property.

Plugin order
    TBD... But in most cases you will want this plugin to run last.

Testing your match patterns
    Online Regexp testers like http://www.regexplanet.com/advanced/java/index.html
    can help get the basics of your pattern working.
    To test in nutch: 
        Prepare a test HTML file with the field contents you want to test. 
        Place this in a directory accessible to nutch.
        Use the file:/// syntax to list the test file(s) in a test/urls seed list.
        See the nutch faq "index my local file system" for conf settings you will need.
        (Note the urlmatch and hostmatch patterns may not conform to your test file host and url; This
        test approach confirms only how your global matches behave, unless your urlmatch and hostmatch
        patterns also match the file: URL pattern)
 
    Run..
        bin/nutch inject crawl/crawldb test
        bin/nutch generate crawl/crawldb crawl/segments
        bin/nutch fetch crawl/segments/[segment]
        bin/nutch parse crawl/segments/[segment]

    To inspect the returned fields...
        bin/nutch readseg -dump crawl/segments/[segment] testout
        less testout/dump

    To retry: delete crawl/segments/[segment]/crawl_parse and repeat the parse and dump step.