/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.hydrator.plugin;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.action.Action;
import co.cask.cdap.etl.api.action.ActionContext;
import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;


/**
 * File Contents Action Plugin - Checks files for a specified regular expression
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name(FileContentsAction.PLUGIN_NAME)
@Description("Checks if a file is empty and the contents of the file if needed.")
public class FileContentsAction extends Action {
  private static final Logger LOG = LoggerFactory.getLogger(FileContentsAction.class);

  public static final String PLUGIN_NAME = "FileContents";

  private final FileContentsConfig config;

  public FileContentsAction(FileContentsConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    config.validate();
  }

  @Override
  public void run(ActionContext context) throws Exception {
    config.validate();
    Path source = new Path(config.sourceFilePath);
    List<Pattern> fileContentsRegexes = new ArrayList();
    if (!Strings.isNullOrEmpty(config.fileContentsRegex)) {
      String[] splits = config.fileContentsRegex.split("~");
      for (String fileContentsRegexString : splits) {
        fileContentsRegexes.add(Pattern.compile(fileContentsRegexString));
      }
    }

    FileSystem fileSystem = source.getFileSystem(new Configuration());

    // Convert a single file
    if (fileSystem.exists(source) && fileSystem.getFileStatus(source).isFile()) {
      if (config.failOnEmptyFile && fileSystem.getFileStatus(source).getLen() == 0) {
        throw new EmptyFileException(String.format("Empty file %s",
                                                 source.toString()));
      }
      if (fileContentsRegexes.size() > 0 &&
          !hasContentsSingleFile(source, fileSystem, fileContentsRegexes)) {
        throw new MissingContentsException(String.format("The pattern %s was not found in file %s",
                                                 config.fileContentsRegex,
                                                 source.toString()));
      }
    } else {
      // Convert all the files in a directory
      PathFilter filter = new PathFilter() {
        private final Pattern pattern =
          Pattern.compile(config.fileRegex == null ? ".*" : config.fileRegex);

        @Override
        public boolean accept(Path path) {
          return pattern.matcher(path.getName()).matches();
        }
      };
      FileStatus[] listFiles = fileSystem.globStatus(source, filter);
      if (listFiles == null || listFiles.length == 0 || (listFiles.length == 1 && listFiles[0].isDirectory())) {
        // try again without globbing action
        listFiles = fileSystem.listStatus(source, filter);
      }

      if (listFiles.length == 0) {
        LOG.warn("Not converting any files from source {} matching regular expression",
                 source.toString(), config.fileRegex);
      }
      for (FileStatus file : listFiles) {
        if (!file.isDirectory()) { // ignore directories
          source = file.getPath();
          if (config.failOnEmptyFile && fileSystem.getFileStatus(source).getLen() == 0) {
            throw new EmptyFileException(String.format("Empty file %s",
                                                     source.toString()));
          }
          if (fileContentsRegexes.size() > 0 &&
              !hasContentsSingleFile(source, fileSystem, fileContentsRegexes)) {
            throw new MissingContentsException(String.format("The pattern %s was not found in file %s",
                                                     config.fileContentsRegex,
                                                     source.toString()));
          }
        }
      }
    }
  }

  private boolean hasContentsSingleFile(Path source, FileSystem fileSystem,
                                        List<Pattern> fileContentsPattern) throws IOException {
    boolean[] found = new boolean[fileContentsPattern.size()];
    try (BufferedReader br = new BufferedReader(new InputStreamReader(fileSystem.open(source)))) {
      String line;
      while ((line = br.readLine()) != null) {
        for (int i = 0; i < fileContentsPattern.size(); i++) {
          Matcher matcher = fileContentsPattern.get(i).matcher(line);
          if (matcher.matches()) {
            found[i] = true;
          }
        }
      }
    } catch (IOException e) {
      throw new IOException(String.format("Failed treading file %s", source.toString()), e);
    }
    for (int i = 0; i < found.length; i++) {
      if (!found[i]) {
        return false;
      }
    }
    return true;
  }

  /**
   * An exception for empty files
   */
  public class EmptyFileException extends RuntimeException {
    public EmptyFileException(String message) {
      super(message);
    }
  }

  /**
   * An exception for missing contents of files
   */
  public class MissingContentsException extends RuntimeException {
    public MissingContentsException(String message) {
      super(message);
    }
  }

  /**
   * Config class that contains all properties required for running the unload command.
   */
  public static class FileContentsConfig extends PluginConfig {
    @Macro
    @Description("The source location where the file or files live. You can use glob syntax here such as *.dat.")
    private String sourceFilePath;

    @Macro
    @Nullable
    @Description("A regular expression for filtering files such as .*\\.txt")
    private String fileRegex;

    @Macro
    @Nullable
    @Description("A regular expression for checking the contents of the file.")
    private String fileContentsRegex;

    @Macro
    @Description("Set to true if this plugin should fail if the file is empty.")
    private Boolean failOnEmptyFile;


    public FileContentsConfig(String sourceFilePath, String fileRegex,
                              String fileContentsRegex,
                              Boolean failOnEmptyFile) {
      this.sourceFilePath = sourceFilePath;
      this.fileRegex = fileRegex;
      this.fileContentsRegex = fileContentsRegex;
      this.failOnEmptyFile = failOnEmptyFile;
    }

    /**
     * Validates the config parameters required for unloading the data.
     */
    private void validate() throws IllegalArgumentException {
      try {
        if (!Strings.isNullOrEmpty(fileRegex)) {
          Pattern.compile(fileRegex);
        }
      } catch (Exception e) {
        throw new IllegalArgumentException("The regular expression pattern provided to match files " +
                                             "is not a valid regular expression.", e);
      }
      try {
        if (!failOnEmptyFile && !Strings.isNullOrEmpty(fileContentsRegex)) {
          String[] regexes = fileContentsRegex.split("~");
          for (String regex : regexes) {
            Pattern.compile(regex);
          }
        }
      } catch (Exception e) {
        throw new IllegalArgumentException("The regular expression pattern provided to check file contents " +
                                             "is not a valid regular expression.", e);
      }
      if (Strings.isNullOrEmpty(sourceFilePath)) {
        throw new IllegalArgumentException("Source file or folder is required.");
      }
      try {
        Path source = new Path(sourceFilePath);
        source.getFileSystem(new Configuration());
      } catch (IOException e) {
        throw new IllegalArgumentException("Cannot determine the file system of the source file.", e);
      }
    }
  }
}
