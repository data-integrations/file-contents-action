/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.plugin.filecontent;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.action.ActionContext;
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
    FailureCollector failureCollector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    config.validate(failureCollector);
  }

  @Override
  public void run(ActionContext context) throws Exception {
    FailureCollector failureCollector = context.getFailureCollector();
    config.validate(failureCollector);
    failureCollector.getOrThrowException();

    Path source = new Path(config.getSourceFilePath());
    List<Pattern> fileContentsRegexes = new ArrayList();
    if (!Strings.isNullOrEmpty(config.getFileContentsRegex())) {
      String[] splits = config.getFileContentsRegex().split("~");
      for (String fileContentsRegexString : splits) {
        fileContentsRegexes.add(Pattern.compile(fileContentsRegexString));
      }
    }

    FileSystem fileSystem = source.getFileSystem(new Configuration());

    // Convert a single file
    if (fileSystem.exists(source) && fileSystem.getFileStatus(source).isFile()) {
      if (config.getFailOnEmptyFile() && fileSystem.getFileStatus(source).getLen() == 0) {
        throw new EmptyFileException(String.format("Empty file %s",
                                                 source.toString()));
      }
      if (fileContentsRegexes.size() > 0 &&
          !hasContentsSingleFile(source, fileSystem, fileContentsRegexes)) {
        throw new MissingContentsException(String.format("The pattern %s was not found in file %s",
                                                 config.getFileContentsRegex(),
                                                 source.toString()));
      }
    } else {
      // Convert all the files in a directory
      PathFilter filter = new PathFilter() {
        private final Pattern pattern =
          Pattern.compile(config.getFileRegex() == null ? ".*" : config.getFileRegex());

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
                 source.toString(), config.getFileRegex());
      }
      for (FileStatus file : listFiles) {
        if (!file.isDirectory()) { // ignore directories
          source = file.getPath();
          if (config.getFailOnEmptyFile() && fileSystem.getFileStatus(source).getLen() == 0) {
            throw new EmptyFileException(String.format("Empty file %s",
                                                     source.toString()));
          }
          if (fileContentsRegexes.size() > 0 &&
              !hasContentsSingleFile(source, fileSystem, fileContentsRegexes)) {
            throw new MissingContentsException(String.format("The pattern %s was not found in file %s",
                                                     config.getFileContentsRegex(),
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

}
