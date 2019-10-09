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

import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class FileContentsConfigTest {
  private static final String MOCK_STAGE = "mockStage";
  private static final FileContentsConfig VALID_CONFIG = new FileContentsConfig(
    "test/path",
    null,
    null,
    false
  );

  @Test
  public void testValidConfig() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    VALID_CONFIG.validate(failureCollector);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testValidateFileRegexp() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    FileContentsConfig config = FileContentsConfig.builder(VALID_CONFIG)
      .setFileRegex("[")
      .build();
    List<List<String>> paramNames = Collections.singletonList(
      Collections.singletonList(FileContentsConfig.FILE_REGEX)
    );

    config.validate(failureCollector);
    assertValidationFailed(failureCollector, paramNames);
  }

  @Test
  public void testValidateFileContentsRegex() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    FileContentsConfig config = FileContentsConfig.builder(VALID_CONFIG)
      .setFileContentsRegex("[")
      .build();
    List<List<String>> paramNames = Collections.singletonList(
      Collections.singletonList(FileContentsConfig.FILE_CONTENTS_REGEX)
    );

    config.validate(failureCollector);
    assertValidationFailed(failureCollector, paramNames);
  }

  @Test
  public void testValidateFileContentsRegexAndFailOnEmptyFileTrue() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    FileContentsConfig config = FileContentsConfig.builder(VALID_CONFIG)
      .setFileContentsRegex("[")
      .setFailOnEmptyFile(true)
      .build();

    config.validate(failureCollector);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  private void assertValidationFailed(MockFailureCollector failureCollector, List<List<String>> paramNames) {
    List<ValidationFailure> failureList = failureCollector.getValidationFailures();
    Assert.assertEquals(paramNames.size(), failureList.size());
    Iterator<List<String>> paramNameIterator = paramNames.iterator();
    failureList.stream().map(failure -> failure.getCauses()
      .stream()
      .filter(cause -> cause.getAttribute(CauseAttributes.STAGE_CONFIG) != null)
      .collect(Collectors.toList()))
      .filter(causeList -> paramNameIterator.hasNext())
      .forEach(causeList -> {
        List<String> parameters = paramNameIterator.next();
        Assert.assertEquals(parameters.size(), causeList.size());
        IntStream.range(0, parameters.size()).forEach(i -> {
          ValidationFailure.Cause cause = causeList.get(i);
          Assert.assertEquals(parameters.get(i), cause.getAttribute(CauseAttributes.STAGE_CONFIG));
        });
      });
  }
}
