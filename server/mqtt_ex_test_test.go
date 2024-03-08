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
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nuid"
)

type mqttExDial string

type mqttExTarget struct {
	singleServers []*Server
	clusters      []*cluster
	configs       []mqttExTestConfig
	all           []mqttExDial
}

type mqttExTestConfig struct {
	name string
	pub  []mqttExDial
	sub  []mqttExDial
}

func TestMQTTExCompliance(t *testing.T) {
	if mqttexCLICommandPath == _EMPTY_ {
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
	if mqttexTestCommandPath == _EMPTY_ {
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
			defer target.Shutdown()

			// initialize the MQTT assets by "touching" all nodes in the
			// cluster, but then reload to start with fresh server state.
			for _, dial := range target.all {
				mqttExInitServer(t, dial)
			}

			numRMS := 64
			strSize := strconv.Itoa(512)
			strNumRMS := strconv.Itoa(numRMS)
			topics := make([]string, len(target.configs))

			// Publish and check all sub nodes for retained messages. Store the
			// topics to check again, after a reboot.
			for i, tc := range target.configs {
				topic := "subret_" + nuid.Next()
				topics[i] = topic

				args := []string{
					"--retained", strNumRMS,
					"--topic", topic,
					"--size", strSize,
				}
				for _, dial := range tc.pub {
					args = append(args, "--pub-server", string(dial))
				}

				mqttexRunTest(t, "subret", tc.sub, args...)
			}

			// Reload the target
			target.Reload(t)

			// Now check again (explicitly include the subtopics, subret did it implicitly)
			for i, tc := range target.configs {
				topic := topics[i]
				if numRMS > 1 {
					topic += "/+"
				}
				mqttexRunTestRetry(t, 1, "sub", tc.sub,
					"--retained", strNumRMS,
					"--qos", "0",
					"--topic", topic,
				)
			}
		})
	}
}

func mqttExInitServer(tb testing.TB, dial mqttExDial) {
	tb.Helper()
	mqttexRunTestRetry(tb, 3, "pubsub", []mqttExDial{dial}, "--timeout", "4s")
}

func mqttExNewDialForServer(s *Server, username, password string) mqttExDial {
	o := s.getOpts().MQTT
	return mqttExNewDial(username, password, o.Host, o.Port, s.Name())
}

func mqttExNewDial(username, password, host string, port int, comment string) mqttExDial {
	d := ""
	switch {
	case username != "" && password != "":
		d = fmt.Sprintf("%s:%s@%s:%d", username, password, host, port)
	case username != "":
		d = fmt.Sprintf("%s@%s:%d", username, host, port)
	default:
		d = fmt.Sprintf("%s:%d", host, port)
	}
	if comment != "" {
		d += "#" + comment
	}
	return mqttExDial(d)
}

func (d mqttExDial) Get() (u, p, s, c string) {
	if d == "" {
		return "", "", "127.0.0.1:1883", ""
	}
	in := string(d)
	if i := strings.LastIndex(in, "#"); i != -1 {
		c = in[i+1:]
		in = in[:i]
	}
	if i := strings.LastIndex(in, "@"); i != -1 {
		up := in[:i]
		in = in[i+1:]
		u = up
		if i := strings.Index(up, ":"); i != -1 {
			u = up[:i]
			p = up[i+1:]
		}
	}
	s = in
	return u, p, s, c
}

func (d mqttExDial) Name() string {
	_, _, _, c := d.Get()
	return c
}

func (t *mqttExTarget) Reload(tb testing.TB) {
	tb.Helper()

	for _, c := range t.clusters {
		c.stopAll()
		c.restartAllSamePorts()
		// We call mqttExInitServer on each server in the reloaded target to make
		// sure they are ready. Unfortunately it appears that our JS APIs time out
		// (after 4 seconds) when used immediately after a (cluster) reload. Giving
		// it a bit of time here seems to reduce the occurence of the timeouts and
		// thus reduce the overall test time.
		time.Sleep(1000 * time.Millisecond)
	}

	for i, s := range t.singleServers {
		o := s.getOpts()
		s.Shutdown()
		t.singleServers[i] = testMQTTRunServer(tb, o)
	}

	for _, dial := range t.all {
		mqttExInitServer(tb, dial)
	}
}

func (t *mqttExTarget) Shutdown() {
	for _, c := range t.clusters {
		c.shutdown()
	}
	for _, s := range t.singleServers {
		testMQTTShutdownServer(s)
	}
}

func mqttExMakeServer(tb testing.TB) *mqttExTarget {
	tb.Helper()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(tb, o)
	all := []mqttExDial{mqttExNewDialForServer(s, "", "")}
	return &mqttExTarget{
		singleServers: []*Server{s},
		all:           all,
		configs: []mqttExTestConfig{
			{
				name: "single server",
				pub:  all,
				sub:  all,
			},
		},
	}
}

func mqttExMakeServerWithLeafnode(hubd, leafd string, connectSystemAccount bool) func(tb testing.TB) *mqttExTarget {
	return func(tb testing.TB) *mqttExTarget {
		tb.Helper()

		if hubd != "" {
			hubd = "domain: " + hubd + ", "
		}
		sconf := `
listen: 127.0.0.1:-1

server_name: HUB
jetstream: {max_mem_store: 256MB, max_file_store: 2GB, ` + hubd + `store_dir: '` + tb.TempDir() + `'}

leafnodes {
	listen: 127.0.0.1:-1
}

accounts {
	ONE { users = [ { user: "one", pass: "p" } ]; jetstream: enabled }
	$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
}

mqtt {
	listen: 127.0.0.1:-1
}
`
		hubConf := createConfFile(tb, []byte(sconf))
		hubServer, o := RunServerWithConfig(hubConf)
		leafRemoteAddr := fmt.Sprintf("%s:%d", o.LeafNode.Host, o.LeafNode.Port)
		tb.Cleanup(func() {
			os.Remove(hubConf)
		})

		sysRemote := ""
		if connectSystemAccount {
			sysRemote = `{ url: "nats://admin:s3cr3t!@` + leafRemoteAddr + `", account: "$SYS" },` + "\n\t\t"
		}
		if leafd != "" {
			leafd = "domain: " + leafd + ", "
		}
		leafconf := `
listen: 127.0.0.1:-1

server_name: SPOKE
jetstream: {max_mem_store: 256MB, max_file_store: 2GB, ` + leafd + `store_dir: '` + tb.TempDir() + `'}

leafnodes {
	remotes = [
		` + sysRemote + `{ url: "nats://one:p@` + leafRemoteAddr + `", account: "ONE" },
	]
}

accounts {
	ONE { users = [ { user: "one", pass: "p" } ]; jetstream: enabled }
	$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
}

mqtt {
	listen: 127.0.0.1:-1
}
`
		leafConf := createConfFile(tb, []byte(leafconf))
		leafServer, _ := RunServerWithConfig(leafConf)
		tb.Cleanup(func() {
			os.Remove(leafConf)
		})

		both := []mqttExDial{
			mqttExNewDialForServer(hubServer, "one", "p"),
			mqttExNewDialForServer(leafServer, "one", "p"),
		}
		return &mqttExTarget{
			singleServers: []*Server{hubServer, leafServer},
			all:           both,
			configs: []mqttExTestConfig{
				{name: "pub to all", pub: both, sub: both},
				{name: "pub to SPOKE", pub: both[1:], sub: both},
				{name: "pub to HUB", pub: both[:1], sub: both},
			},
		}
	}
}

func mqttExMakeCluster(size int, domain string) func(tb testing.TB) *mqttExTarget {
	return func(tb testing.TB) *mqttExTarget {
		tb.Helper()
		if size < 3 {
			tb.Fatal("cluster size must be at least 3")
		}

		if domain != "" {
			domain = "domain: " + domain + ", "
		}
		clusterConf := `
	listen: 127.0.0.1:-1

	server_name: %s
	jetstream: {max_mem_store: 256MB, max_file_store: 2GB, ` + domain + `store_dir: '%s'}

	leafnodes {
		listen: 127.0.0.1:-1
	}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	mqtt {
		listen: 127.0.0.1:-1
		stream_replicas: 3
	}

	accounts {
		ONE { users = [ { user: "one", pass: "p" } ]; jetstream: enabled }
		$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
	}
`
		cl := createJetStreamClusterWithTemplate(tb, clusterConf, "MQTT", size)
		cl.waitOnLeader()

		all := []mqttExDial{}
		for _, s := range cl.servers {
			all = append(all, mqttExNewDialForServer(s, "one", "p"))
		}

		return &mqttExTarget{
			clusters: []*cluster{cl},
			all:      all,
			configs: []mqttExTestConfig{
				{
					name: "publish to one",
					pub: []mqttExDial{
						mqttExNewDialForServer(cl.randomServer(), "one", "p"),
					},
					sub: all,
				},
				{
					name: "publish to all",
					pub:  all,
					sub:  all,
				},
			},
		}
	}
}

func mqttExMakeClusterWithLeafnodeCluster(hubd, leafd string, connectSystemAccount bool) func(tb testing.TB) *mqttExTarget {
	return func(tb testing.TB) *mqttExTarget {
		tb.Helper()

		// Create HUB cluster.
		if hubd != "" {
			hubd = "domain: " + hubd + ", "
		}
		hubConf := `
	listen: 127.0.0.1:-1

	server_name: %s
	jetstream: {max_mem_store: 256MB, max_file_store: 2GB, ` + hubd + `store_dir: '%s'}

	leafnodes {
		listen: 127.0.0.1:-1
	}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	mqtt {
		listen: 127.0.0.1:-1
		stream_replicas: 3
	}

	accounts {
		ONE { users = [ { user: "one", pass: "p" } ]; jetstream: enabled }
		$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
	}
`
		hub := createJetStreamClusterWithTemplate(tb, hubConf, "HUB", 3)
		hub.waitOnLeader()

		// Pick a host to connect leafnodes to
		lno := hub.randomNonLeader().getOpts().LeafNode
		leafRemoteAddr := fmt.Sprintf("%s:%d", lno.Host, lno.Port)
		hubRandom := mqttExNewDialForServer(hub.randomNonLeader(), "one", "p")
		hubAll := []mqttExDial{}
		for _, s := range hub.servers {
			hubAll = append(hubAll, mqttExNewDialForServer(s, "one", "p"))
		}

		// Create SPOKE (leafnode) cluster.
		sysRemote := ""
		if connectSystemAccount {
			sysRemote = `{ url: "nats://admin:s3cr3t!@` + leafRemoteAddr + `", account: "$SYS" },` + "\n\t\t\t"
		}
		if leafd != "" {
			leafd = "domain: " + leafd + ", "
		}
		leafConf := `
	listen: 127.0.0.1:-1

	server_name: %s
	jetstream: {max_mem_store: 256MB, max_file_store: 2GB, ` + leafd + `store_dir: '%s'}

	leafnodes {
		remotes = [
			` + sysRemote + `{ url: "nats://one:p@` + leafRemoteAddr + `", account: "ONE" },
		]
	}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	mqtt {
		listen: 127.0.0.1:-1
		stream_replicas: 3
	}

	accounts {
		ONE { users = [ { user: "one", pass: "p" } ]; jetstream: enabled }
		$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
	}
`
		spoke := createJetStreamCluster(tb, leafConf, "SPOKE", "SPOKE-", 3, 22111, false)
		expectedConnections := 2
		if !connectSystemAccount {
			expectedConnections = 1
		}
		for _, s := range spoke.servers {
			checkLeafNodeConnectedCount(tb, s, expectedConnections)
		}
		spoke.waitOnPeerCount(3)
		spokeRandom := mqttExNewDialForServer(spoke.randomNonLeader(), "one", "p")
		spokeAll := []mqttExDial{}
		for _, s := range spoke.servers {
			spokeAll = append(spokeAll, mqttExNewDialForServer(s, "one", "p"))
		}

		all := append(hubAll, spokeAll...)

		return &mqttExTarget{
			clusters: []*cluster{hub, spoke},
			all:      all,
			configs: []mqttExTestConfig{
				{name: "publish to all", pub: all, sub: all},
				{name: "publish to all hub", pub: hubAll, sub: all},
				{name: "publish to random in hub", pub: []mqttExDial{hubRandom}, sub: all},
				{name: "publish to all spoke", pub: spokeAll, sub: all},
				{name: "publish to random in spoke", pub: []mqttExDial{spokeRandom}, sub: all},
			},
		}
	}
}

var mqttexCLICommandPath = func() string {
	p := os.Getenv("MQTT_CLI")
	if p == "" {
		p, _ = exec.LookPath("mqtt")
	}
	return p
}()

var mqttexTestCommandPath = func() string {
	p, _ := exec.LookPath("mqtt-test")
	return p
}()

func mqttexRunTest(tb testing.TB, subCommand string, dials []mqttExDial, extraArgs ...string) MQTTBenchmarkResult {
	tb.Helper()
	return mqttexRunTestRetry(tb, 1, subCommand, dials, extraArgs...)
}

func mqttexRunTestRetry(tb testing.TB, n int, subCommand string, dials []mqttExDial, extraArgs ...string) MQTTBenchmarkResult {
	tb.Helper()
	var err error
	var r MQTTBenchmarkResult
	for i := 0; i < n; i++ {
		if r, err = mqttexTryTest(tb, subCommand, dials, extraArgs...); err == nil {
			return r
		}

		if i < (n - 1) {
			tb.Logf("failed to %q %s on attempt %v, will retry %v more times. Error: %v", subCommand, extraArgs, i, n-i-1, err)
		} else {
			tb.Fatalf("failed to %q %s last attempt %v. Error: %v", subCommand, extraArgs, i, err)
		}
	}
	return nil
}

func mqttexTryTest(tb testing.TB, subCommand string, dials []mqttExDial, extraArgs ...string) (MQTTBenchmarkResult, error) {
	tb.Helper()

	if mqttexTestCommandPath == "" {
		tb.Skip(`"mqtt-test" command is not found in $PATH.`)
	}

	// the "-v" here may be useful in case of errors, but also there appear to
	// be bugs in the paho client that I couldn't track down.
	args := []string{subCommand, "-v"}
	for _, dial := range dials {
		args = append(args, "-s", string(dial))
	}
	args = append(args, extraArgs...)
	cmd := exec.Command(mqttexTestCommandPath, args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("error executing %q: %v", cmd.String(), err)
	}
	defer stdout.Close()
	errbuf := bytes.Buffer{}
	cmd.Stderr = &errbuf
	if err = cmd.Start(); err != nil {
		return nil, fmt.Errorf("error executing %q: %v", cmd.String(), err)
	}
	out, err := io.ReadAll(stdout)
	if err != nil {
		return nil, fmt.Errorf("error executing %q: failed to read output: %v", cmd.String(), err)
	}
	if err = cmd.Wait(); err != nil {
		return nil, fmt.Errorf("error executing %q: %v\n\n%s\n\n%s", cmd.String(), err, string(out), errbuf.String())
	}

	r := MQTTBenchmarkResult{}
	if err := json.Unmarshal(out, &r); err != nil {
		tb.Fatalf("error executing %q: failed to decode output: %v\n\n%s\n\n%s", cmd.String(), err, string(out), errbuf.String())
	}
	return r, nil
}
