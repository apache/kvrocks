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

package util

type ConfigOptions struct {
	Name    string
	Options []string
}

type KvrocksServerConfigs map[string]string

// / GenerateConfigsMatrix generates all possible combinations of config options
func GenerateConfigsMatrix(configOptions []ConfigOptions) ([]KvrocksServerConfigs, error) {
	configsMatrix := make([]KvrocksServerConfigs, 0)

	var helper func(configs []ConfigOptions, index int, currentConfig map[string]string) error

	helper = func(configs []ConfigOptions, currentIndex int, currentConfig map[string]string) error {
		if currentIndex == len(configOptions) {
			config := make(KvrocksServerConfigs, len(currentConfig))
			for k, v := range currentConfig {
				config[k] = v
			}
			configsMatrix = append(configsMatrix, config)
			return nil
		}

		configName := configs[currentIndex].Name
		originalValue, hadOriginalValue := currentConfig[configName]

		for _, option := range configs[currentIndex].Options {
			currentConfig[configName] = option
			err := helper(configs, currentIndex+1, currentConfig)
			if err != nil {
				return err
			}
		}

		if hadOriginalValue {
			currentConfig[configName] = originalValue
		} else {
			delete(currentConfig, configName)
		}

		return nil
	}

	err := helper(configOptions, 0, make(KvrocksServerConfigs, 0))

	return configsMatrix, err
}
