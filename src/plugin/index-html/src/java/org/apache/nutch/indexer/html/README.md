Index-html Plugin for apache nutch 2.x
=================

Index HTML content of the pages in Apaache Nutch 2.x ( 2.2.1 )

##Instruction: 

- ###Compile from Source

 - Download the plugin folder "index-html" and copy it to you Apache nutch 2 plugin directory ( ex: apache-nutch-2.2.1/src/plugin )
 - Add the ( index-html ) plugin to The plugin folder build.xml ( apache-nutch-2.2.1/src/plugin/build.xml ) in target ( deploy and clean ) so the file will look like 
 ```xml
  <target name="deploy">
     .......
     <ant dir="index-basic" target="deploy"/>
     <ant dir="index-more" target="deploy"/>
     <ant dir="index-html" target="deploy"/>
     <ant dir="language-identifier" target="deploy"/>
    .........
  </target>

  <target name="clean">
     .......
     <ant dir="index-basic" target="deploy"/>
     <ant dir="index-more" target="deploy"/>
     <ant dir="index-html" target="deploy"/>
     <ant dir="language-identifier" target="deploy"/>
    .........
  </target>
  
  ```
  
  - Run ( ant runtime ) in apache nutch 2 root folder to start the build
  - You should have index-html.jar in build folder
  - Enable the plugin by adding it to nutch-sites.xml ( or nutch-default.xml ) like beloe :
  ```xml
      <configuration>
          ..........
          <property>
              <name>plugin.includes</name>
              <value>...........someplugins....|index-html</value>
          </property>
          ..........
      </configuration>
  ```
  - The plugin will add new Field "rawcontent" to the Nutch Doc, To index this field you need to add it to ( scheme.xml or schema-solr4.xml ) like 
  ```xml
    <field name="rawcontent" type="text" sstored="true" indexed="true" multiValued="false"/>
  ```
  - Run the crawler and you should see the new field rawcontent in index!
  
- ###Use Pre-Compiled Library 
  - In The repo there is Build folder contain compiled .jar library ready for use. 
  - Copy the library to your runtime path local if you are running the plugin locally ( apache-nutch-2.2.1/runtime/local/plugins ) Then follow the above steps to configure nutch-sites.xml
  

###Screen Shot 
![Screen Shot](http://i.imgur.com/rPWAcoQ.png)

## Need Help ?

I'm Always glad ot help and assist. so if you have an idea that could make this project better

Submit git issue or contact me [www.meabed.net](http://meabed.net)


### How to contribute

Make a fork, commit to develop branch and make a pull request
