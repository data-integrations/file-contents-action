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

import co.cask.cdap.etl.mock.common.MockPipelineConfigurer;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileFilter;
import java.net.URL;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link FileContentsAction}
 */
public class FileContentsActionTest {
  private static final String EMPTY_FILE = "empty.txt";
  private static final String FILE_WITH_CONTENT_TO_MATCH = "to_match.txt";

  private FileFilter filter = new FileFilter() {
    private final Pattern pattern = Pattern.compile("[^\\.].*\\.utf8");

    @Override
    public boolean accept(File pathname) {
      return pattern.matcher(pathname.getName()).matches();
    }
  };


  @Test(expected = FileContentsAction.EmptyFileException.class)
  public void testSingleEmptyFile() throws Exception {
    ClassLoader classLoader = getClass().getClassLoader();
    URL emptyFile = classLoader.getResource(EMPTY_FILE);
    FileContentsAction.FileContentsConfig config = new FileContentsAction.FileContentsConfig(emptyFile.getFile(),
                                                                                             null, null, true);
    MockPipelineConfigurer configurer = new MockPipelineConfigurer(null);
    new FileContentsAction(config).configurePipeline(configurer);
    new FileContentsAction(config).run(null);
  }

  @Test
  public void testSingleEmptyFileSuccess() throws Exception {
    ClassLoader classLoader = getClass().getClassLoader();
    URL emptyFile = classLoader.getResource(FILE_WITH_CONTENT_TO_MATCH);
    FileContentsAction.FileContentsConfig config = new FileContentsAction.FileContentsConfig(emptyFile.getFile(),
                                                                                             null, null, true);
    MockPipelineConfigurer configurer = new MockPipelineConfigurer(null);
    new FileContentsAction(config).configurePipeline(configurer);
    new FileContentsAction(config).run(null);
  }

  @Test
  public void testSingleFile() throws Exception {
    ClassLoader classLoader = getClass().getClassLoader();
    URL emptyFile = classLoader.getResource(FILE_WITH_CONTENT_TO_MATCH);
    FileContentsAction.FileContentsConfig config = new FileContentsAction.FileContentsConfig(emptyFile.getFile(),
                                                                                             null, ".*", true);
    MockPipelineConfigurer configurer = new MockPipelineConfigurer(null);
    new FileContentsAction(config).configurePipeline(configurer);
    new FileContentsAction(config).run(null);
  }

  @Test(expected = FileContentsAction.MissingContentsException.class)
  public void testSingleFileFailure() throws Exception {
    ClassLoader classLoader = getClass().getClassLoader();
    URL emptyFile = classLoader.getResource(FILE_WITH_CONTENT_TO_MATCH);
    FileContentsAction.FileContentsConfig config =
      new FileContentsAction.FileContentsConfig(emptyFile.getFile(), null, "not[0-9]there", true);
    MockPipelineConfigurer configurer = new MockPipelineConfigurer(null);
    new FileContentsAction(config).configurePipeline(configurer);
    new FileContentsAction(config).run(null);
  }


  @Test
  public void testFolder() throws Exception {
    ClassLoader classLoader = getClass().getClassLoader();
    URL emptyFile = classLoader.getResource(FILE_WITH_CONTENT_TO_MATCH);

    FileContentsAction.FileContentsConfig config =
      new FileContentsAction.FileContentsConfig(new File(emptyFile.getFile()).getParent(),
                                                "to_match.*", ".*need.*", true);
    MockPipelineConfigurer configurer = new MockPipelineConfigurer(null);
    new FileContentsAction(config).configurePipeline(configurer);
    new FileContentsAction(config).run(null);
  }

  @Test(expected = FileContentsAction.MissingContentsException.class)
  public void testFolderFail() throws Exception {
    ClassLoader classLoader = getClass().getClassLoader();
    URL emptyFile = classLoader.getResource(FILE_WITH_CONTENT_TO_MATCH);

    FileContentsAction.FileContentsConfig config =
      new FileContentsAction.FileContentsConfig(new File(emptyFile.getFile()).getParent(),
                                                "to_match.*", ".*not there.*", true);
    MockPipelineConfigurer configurer = new MockPipelineConfigurer(null);
    new FileContentsAction(config).configurePipeline(configurer);
    new FileContentsAction(config).run(null);
  }
}
