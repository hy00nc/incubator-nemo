/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.client;

import org.apache.nemo.runtime.common.state.PlanState;

/**
 * A class provides the translation between the state of plan and corresponding {@link ClientEndpoint}.
 */
public interface StateTranslator {

  /**
   * Translate a plan state of nemo to a corresponding client endpoint state.
   *
   * @param planState to translate.
   * @return the translated state.
   */
  Enum translateState(PlanState.State planState);
}
