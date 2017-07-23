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

Usage Notes
-----------

This plugin does not search across lines in a file. This plugin will only work on UTF-8 text files, not binary.