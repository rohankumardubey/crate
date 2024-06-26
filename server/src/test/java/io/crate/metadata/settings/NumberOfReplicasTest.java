/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.metadata.settings;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.elasticsearch.cluster.metadata.AutoExpandReplicas;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

public class NumberOfReplicasTest {

    @Test
    public void testFromEmptySettings() throws Exception {
        String numberOfResplicas = NumberOfReplicas.getVirtualValue(Settings.EMPTY);
        assertThat(numberOfResplicas).isEqualTo("1");
    }

    @Test
    public void testNumber() throws Exception {
        String numberOfResplicas = NumberOfReplicas.getVirtualValue(Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 4)
            .build());
        assertThat(numberOfResplicas).isEqualTo("4");
    }

    @Test
    public void testAutoExpandSettingsTakePrecedence() throws Exception {
        String numberOfResplicas = NumberOfReplicas.getVirtualValue(Settings.builder()
            .put(AutoExpandReplicas.SETTING_KEY, "0-all")
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .build());
        assertThat(numberOfResplicas).isEqualTo("0-all");
    }

    @Test
    public void testInvalidAutoExpandSettings() throws Exception {
        assertThatThrownBy(() ->
            NumberOfReplicas.getVirtualValue(Settings.builder()
                .put(AutoExpandReplicas.SETTING_KEY, "abc")
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .build()))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("The \"number_of_replicas\" range \"abc\" isn't valid");
    }
}

