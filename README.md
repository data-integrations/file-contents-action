<a href="https://cdap-users.herokuapp.com/"><img alt="Join CDAP community" src="https://cdap-users.herokuapp.com/badge.svg?t=to-utf8-action"/></a> [![Build Status](https://travis-ci.org/hydrator/to-utf8-action.svg?branch=release/1.0)](https://travis-ci.org/hydrator/to-utf8-action) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) <img alt="CDAP Action" src="https://cdap-users.herokuapp.com/assets/cdap-action.svg"/> []() <img src="https://cdap-users.herokuapp.com/assets/cm-available.svg"/>
# File Contents Action

Description
-----------
This action plugin can be used to check if a file is empty or if the
contents of a file match a given pattern.

Use Case
--------
This action is used if you need to check if a file is empty or has header or footers before processing.

Properties
----------
| Configuration | Required | Default | Description |
| :------------ | :------: | :------ | :---------- |
| **Source Path** | **Y** | None | The full path of the file or directory that is to be converted. In the case of a directory, if fileRegex is set, then only files in the source directory matching the regex expression will be moved. Otherwise, all files in the directory will be moved. For example: `hdfs://hostname/tmp`. You can use globbing syntax here. |
| **File Regular Expression** | **N** | None | Regular expression to filter the files in the source directory that will be moved. This is useful when the globbing syntax in the source directory is not precise enough for your files. |
| **File Contents Regular Expressions** | **N** | None| A list of Regular Expressions that all need to be present in the file otherwise the plugin will throw an exception and stop the pipeline. |
| **Fail if the file is empty?** | **Y** | true | If set to true, the pipeline will fail if the file is empty. Otherwise, it will ignore that check. |


Getting Started
===============

Prerequisites
--------------
CDAP version 4.1.x or higher.

Building Plugins
----------------
You get started with File Contents action plugin by building directly from the latest source code::

   git clone git@github.com:hydrator/file-contents-action.git
   cd file-contents-action
   mvn clean package

After the build completes, you will have a JAR for each plugin under each
``<plugin-name>/target/`` directory.

Deploying Plugins
-----------------
You can deploy a plugin using the CDAP CLI::

  > load artifact <target/plugin-jar> config-file <resources/plugin-config>

  > load artifact target/file-contents-action-<version>.jar \
         config-file target/to-utf8-action-<version>.json

You can build without running tests: ``mvn clean install -DskipTests``

Mailing Lists
-------------
CDAP User Group and Development Discussions:

- `cdap-user@googlegroups.com <https://groups.google.com/d/forum/cdap-user>`__

The *cdap-user* mailing list is primarily for users using the product to develop
applications or building plugins for appplications. You can expect questions from
users, release announcements, and any other discussions that we think will be helpful
to the users.

IRC Channel
-----------
CDAP IRC Channel: #cdap on irc.freenode.net


License and Trademarks
======================

Copyright © 2017 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
either express or implied. See the License for the specific language governing permissions
and limitations under the License.

Cask is a trademark of Cask Data, Inc. All rights reserved.

Apache, Apache HBase, and HBase are trademarks of The Apache Software Foundation. Used with
permission. No endorsement by The Apache Software Foundation is implied by the use of these marks.
