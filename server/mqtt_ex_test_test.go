// Copyright 2024 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build !skip_mqtt_tests
// +build !skip_mqtt_tests

package server

import (
	"fmt"
	"os/exec"
	"strconv"
	"testing"

	"github.com/nats-io/nuid"
)

func TestMQTTExCompliance(t *testing.T) {
	if mqttexCLICommandPath == "" {
		t.Skip(`"mqtt" command is not found in $PATH nor $MQTT_CLI. See https://hivemq.github.io/mqtt-cli/docs/installation/#debian-package for installation instructions`)
	}

	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	o = s.getOpts()
	defer testMQTTShutdownServer(s)

	cmd := exec.Command(mqttexCLICommandPath, "test", "-V", "3", "-p", strconv.Itoa(o.MQTT.Port))

	output, err := cmd.CombinedOutput()
	t.Log(string(output))
	if err != nil {
		if exitError, ok := err.(*exec.ExitError); ok {
			t.Fatalf("mqtt cli exited with error: %v", exitError)
		}
	}
}

func TestMQTTExRetainedMessages(t *testing.T) {
	if mqttexTestCommandPath == "" {
		t.Skip(`"mqtt-test" command is not found in $PATH.`)
	}

	for _, topo := range []struct {
		name  string
		makef func(testing.TB) *mqttExTarget
	}{
		{
			name:  "single server",
			makef: mqttExMakeServer,
		},
		{
			name:  "server with leafnode",
			makef: mqttExMakeServerWithLeafnode("HUBD", "LEAFD", true),
		},
		{
			name:  "server with leafnode no domains",
			makef: mqttExMakeServerWithLeafnode("", "", true),
		},
		{
			name:  "server with leafnode no system account",
			makef: mqttExMakeServerWithLeafnode("HUBD", "LEAFD", false),
		},
		{
			name:  "cluster",
			makef: mqttExMakeCluster(4, ""),
		},
		{
			name:  "cluster with leafnode cluster",
			makef: mqttExMakeClusterWithLeafnodeCluster("HUBD", "LEAFD", true),
		},
		{
			name:  "cluster with leafnode cluster no system account",
			makef: mqttExMakeClusterWithLeafnodeCluster("HUBD", "LEAFD", false),
		},
	} {
		t.Run(topo.name, func(t *testing.T) {
			target := topo.makef(t)
			t.Cleanup(target.Shutdown)

			// initialize the MQTT assets by "touching" all nodes in the cluster, but then reload to start with fresh server state.
			for _, dial := range target.all {
				mqttExInitServer(t, dial)
			}

			numRMS := 10
			strNumRMS := "10"
			for _, tc := range target.configs {
				t.Run(tc.name, func(t *testing.T) {
					topic := "subret_" + nuid.Next()

					// publish retained messages, one at a time, round-robin across pubNodes.
					iNode := 0
					for i := 0; i < numRMS; i++ {
						pubTopic := fmt.Sprintf("%s/%d", topic, i)
						dial := tc.pub[iNode%len(tc.pub)]
						mqttexRunTest(t, "pub", dial,
							"--retain",
							"--topic", pubTopic,
							"--qos", "0",
							"--size", "100",
						)
						iNode++
					}

					for _, dial := range tc.sub {
						_, _, _, name := dial.Get()
						t.Run(fmt.Sprintf("subscribe at %s", name), func(t *testing.T) {
							mqttexRunTest(t, "sub", dial,
								"--retained", strNumRMS,
								"--qos", "0",
								"--topic", topic,
							)
						})
					}

					target.Reload(t)

					for _, dial := range tc.sub {
						_, _, _, name := dial.Get()
						t.Run(fmt.Sprintf("subscribe after reload at %s", name), func(t *testing.T) {
							mqttexRunTest(t, "sub", dial,
								"--retained", strNumRMS,
								"--qos", "0",
								"--topic", topic,
							)
						})
					}
				})
			}
		})
	}
}
