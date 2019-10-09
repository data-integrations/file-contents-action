/*
 * Copyright © 2019 Cask Data, Inc.
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
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import javax.annotation.Nullable;

/**
 * Config class that contains all properties required for running the unload command.
 */
public class FileContentsConfig extends PluginConfig {
  public static final String SOURCE_FILE_PATH = "sourceFilePath";
  public static final String FILE_REGEX = "fileRegex";
  public static final String FILE_CONTENTS_REGEX = "fileContentsRegex";
  public static final String FAIL_ON_EMPTY_FILE = "failOnEmptyFile";

  @Name(SOURCE_FILE_PATH)
  @Macro
  @Description("The source location where the file or files live. You can use glob syntax here such as *.dat.")
  private String sourceFilePath;

  @Name(FILE_REGEX)
  @Macro
  @Nullable
  @Description("A regular expression for filtering files such as .*\\.txt")
  private String fileRegex;

  @Name(FILE_CONTENTS_REGEX)
  @Macro
  @Nullable
  @Description("A regular expression for checking the contents of the file.")
  private String fileContentsRegex;

  @Name(FAIL_ON_EMPTY_FILE)
  @Macro
  @Description("Set to true if this plugin should fail if the file is empty.")
  private Boolean failOnEmptyFile;

  public FileContentsConfig(String sourceFilePath, String fileRegex, String fileContentsRegex,
                            Boolean failOnEmptyFile) {
    this.sourceFilePath = sourceFilePath;
    this.fileRegex = fileRegex;
    this.fileContentsRegex = fileContentsRegex;
    this.failOnEmptyFile = failOnEmptyFile;
  }

  private FileContentsConfig(Builder builder) {
    sourceFilePath = builder.sourceFilePath;
    fileRegex = builder.fileRegex;
    fileContentsRegex = builder.fileContentsRegex;
    failOnEmptyFile = builder.failOnEmptyFile;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(FileContentsConfig copy) {
    return builder()
      .setSourceFilePath(copy.sourceFilePath)
      .setFileRegex(copy.fileRegex)
      .setFileContentsRegex(copy.fileContentsRegex)
      .setFailOnEmptyFile(copy.failOnEmptyFile);
  }

  public String getSourceFilePath() {
    return sourceFilePath;
  }

  @Nullable
  public String getFileRegex() {
    return fileRegex;
  }

  @Nullable
  public String getFileContentsRegex() {
    return fileContentsRegex;
  }

  public Boolean getFailOnEmptyFile() {
    return failOnEmptyFile;
  }

  /**
   * Validates the config parameters required for unloading the data.
   */
  public void validate(FailureCollector failureCollector) {
    try {
      if (!containsMacro(FILE_REGEX) && !Strings.isNullOrEmpty(fileRegex)) {
        Pattern.compile(fileRegex);
      }
    } catch (PatternSyntaxException e) {
      failureCollector.addFailure(
        String.format(
        "The regular expression pattern provided to match files is not a valid regular expression: '%s'",
        e.getMessage()), null)
        .withConfigProperty(FILE_REGEX);
    }
    String currentFileRegex = null;
    try {
      if (!containsMacro(FILE_CONTENTS_REGEX) && !containsMacro(FAIL_ON_EMPTY_FILE)
        && !failOnEmptyFile && !Strings.isNullOrEmpty(fileContentsRegex)) {
        String[] regexes = fileContentsRegex.split("~");
        for (String regex : regexes) {
          currentFileRegex = regex;
          Pattern.compile(regex);
        }
      }
    } catch (PatternSyntaxException e) {
      failureCollector.addFailure(
        String.format(
        "The regular expression pattern '%s' provided to check file contents is not a valid regular expression: '%s'",
        currentFileRegex, e.getMessage()),
        null)
        .withConfigProperty(FILE_CONTENTS_REGEX);
    }
    try {
      if (!containsMacro(SOURCE_FILE_PATH)) {
        Path source = new Path(sourceFilePath);
        source.getFileSystem(new Configuration());
      }
    } catch (IOException e) {
      failureCollector.addFailure(
        "Cannot determine the file system of the source file.",
        null)
        .withStacktrace(e.getStackTrace())
        .withConfigProperty(SOURCE_FILE_PATH);
    }
  }

  /**
   * Builder for FileContentsConfig
   */
  public static final class Builder {
    private String sourceFilePath;
    private String fileRegex;
    private String fileContentsRegex;
    private Boolean failOnEmptyFile;

    private Builder() {
    }

    public Builder setSourceFilePath(String sourceFilePath) {
      this.sourceFilePath = sourceFilePath;
      return this;
    }

    public Builder setFileRegex(String fileRegex) {
      this.fileRegex = fileRegex;
      return this;
    }

    public Builder setFileContentsRegex(String fileContentsRegex) {
      this.fileContentsRegex = fileContentsRegex;
      return this;
    }

    public Builder setFailOnEmptyFile(Boolean failOnEmptyFile) {
      this.failOnEmptyFile = failOnEmptyFile;
      return this;
    }

    public FileContentsConfig build() {
      return new FileContentsConfig(this);
    }
  }
}
