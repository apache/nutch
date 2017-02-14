Parsefilter-regex plugin

Allow parsing and set custom defined fields using regex. Rules can be defined
in a separate rule file or in the nutch configuration.

If a rule file is used, should create a text file regex-parsefilter.txt (which
is the default name of the rules file). To use a different filename, either
update the file value in plugin’s build.xml or add parsefilter.regex.file
config to the nutch config.

ie:
    <property>
      <name>parsefilter.regex.file</name>
      <value>
	/path/to/rulefile
      </value>
    </property


Format of rules: <name>\t<source>\t<regex>\n

ie:
	my_first_field		html	h1
	my_second_field		text	my_pattern


If a rule file is not used, rules can be directly set in the nutch config:

ie:
    <property>
      <name>parsefilter.regex.rules</name>
      <value>
	my_first_field		html	h1
	my_second_field		text	my_pattern
      </value>
    </property

source can be either html or text. If source is html, the regex is applied to
the entire HTML tree. If source is text, the regex is applied to the
extracted text.

