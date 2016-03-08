1. Upgrade dependencies in src/plugin/anthelion/ivy.xml

2. Upgrade plugin dependencies in src/plugin/anthelion/plugin.xml
   To get the list of dependencies and their versions execute:
   $ ant -f ./build-ivy.xml
   $ ls lib | sed 's/^/      <library name="/g' | sed 's/$/"\/>/g'
