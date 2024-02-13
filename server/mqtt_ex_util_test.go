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
	"strings"
	"testing"
)

type testMQTTExDial string

type testMQTTExTarget struct {
	servers  []*Server
	clusters []*cluster
	configs  []testMQTTExTestConfig
	all      []testMQTTExDial
}

type testMQTTExTestConfig struct {
	name string
	pub  []testMQTTExDial
	sub  []testMQTTExDial
}

func testMQTTExInitServer(tb testing.TB, dial testMQTTExDial) {
	tb.Helper()
	mqttexRunTest(tb, "pub", dial, "--id", "__init__")
}

func testMQTTExNewDialForServer(s *Server, username, password string) testMQTTExDial {
	o := s.getOpts().MQTT
	return testMQTTExNewDial(username, password, o.Host, o.Port, s.Name())
}

func testMQTTExNewDial(username, password, host string, port int, comment string) testMQTTExDial {
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
	return testMQTTExDial(d)
}

func (d testMQTTExDial) Get() (u, p, s, c string) {
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

func (t *testMQTTExTarget) Reload(tb testing.TB) {
	tb.Helper()

	for _, c := range t.clusters {
		c.restartAllSamePorts()
	}

	for i, s := range t.servers {
		o := s.getOpts()
		s.Shutdown()
		t.servers[i] = testMQTTRunServer(tb, o)
	}
}

func (t *testMQTTExTarget) Shutdown() {
	for _, c := range t.clusters {
		c.shutdown()
	}
	for _, s := range t.servers {
		testMQTTShutdownServer(s)
	}
}

func testMQTTExMakeServer(tb testing.TB) *testMQTTExTarget {
	tb.Helper()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(tb, o)
	all := []testMQTTExDial{testMQTTExNewDialForServer(s, "", "")}
	return &testMQTTExTarget{
		servers: []*Server{s},
		all:     all,
		configs: []testMQTTExTestConfig{
			{
				name: "single server",
				pub:  all,
				sub:  all,
			},
		},
	}
}

func testMQTTExMakeServerWithLeafnode(hubd, leafd string, connectSystemAccount bool) func(tb testing.TB) *testMQTTExTarget {
	return func(tb testing.TB) *testMQTTExTarget {
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

		both := []testMQTTExDial{
			testMQTTExNewDialForServer(hubServer, "one", "p"),
			testMQTTExNewDialForServer(leafServer, "one", "p"),
		}
		return &testMQTTExTarget{
			servers: []*Server{hubServer, leafServer},
			all:     both,
			configs: []testMQTTExTestConfig{
				{name: "pub to all", pub: both, sub: both},
				{name: "pub to SPOKE", pub: both[1:], sub: both},
				{name: "pub to HUB", pub: both[:1], sub: both},
			},
		}
	}
}

func testMQTTExMakeCluster(size int, domain string) func(tb testing.TB) *testMQTTExTarget {
	return func(tb testing.TB) *testMQTTExTarget {
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

		all := []testMQTTExDial{}
		for _, s := range cl.servers {
			all = append(all, testMQTTExNewDialForServer(s, "one", "p"))
		}

		return &testMQTTExTarget{
			clusters: []*cluster{cl},
			all:      all,
			configs: []testMQTTExTestConfig{
				{
					name: "publish to one",
					pub: []testMQTTExDial{
						testMQTTExNewDialForServer(cl.randomServer(), "one", "p"),
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

func testMQTTExMakeClusterWithLeafnodeCluster(hubd, leafd string, connectSystemAccount bool) func(tb testing.TB) *testMQTTExTarget {
	return func(tb testing.TB) *testMQTTExTarget {
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
		hubRandom := testMQTTExNewDialForServer(hub.randomNonLeader(), "one", "p")
		hubAll := []testMQTTExDial{}
		for _, s := range hub.servers {
			hubAll = append(hubAll, testMQTTExNewDialForServer(s, "one", "p"))
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
		spokeRandom := testMQTTExNewDialForServer(spoke.randomNonLeader(), "one", "p")
		spokeAll := []testMQTTExDial{}
		for _, s := range spoke.servers {
			spokeAll = append(spokeAll, testMQTTExNewDialForServer(s, "one", "p"))
		}

		all := append(hubAll, spokeAll...)

		return &testMQTTExTarget{
			clusters: []*cluster{hub, spoke},
			all:      all,
			configs: []testMQTTExTestConfig{
				{name: "publish to all", pub: all, sub: all},
				{name: "publish to all hub", pub: hubAll, sub: all},
				{name: "publish to random in hub", pub: []testMQTTExDial{hubRandom}, sub: all},
				{name: "publish to all spoke", pub: spokeAll, sub: all},
				{name: "publish to random in spoke", pub: []testMQTTExDial{spokeRandom}, sub: all},
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

func mqttexRunTest(tb testing.TB, subCommand string, dial testMQTTExDial, extraArgs ...string) *MQTTBenchmarkResult {
	tb.Helper()

	if mqttexTestCommandPath == "" {
		tb.Skip(`"mqtt-test" command is not found in $PATH.`)
	}

	args := []string{subCommand, // "-q",
		"-s", string(dial),
	}
	args = append(args, extraArgs...)

	cmd := exec.Command(mqttexTestCommandPath, args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		tb.Fatalf("Error executing %q: %v", cmd.String(), err)
	}
	defer stdout.Close()
	errbuf := bytes.Buffer{}
	cmd.Stderr = &errbuf
	if err = cmd.Start(); err != nil {
		tb.Fatalf("Error executing %q: %v", cmd.String(), err)
	}
	out, err := io.ReadAll(stdout)
	if err != nil {
		tb.Fatalf("Error executing %q: failed to read output: %v", cmd.String(), err)
	}
	if err = cmd.Wait(); err != nil {
		tb.Fatalf("Error executing %q: %v\n\n%s\n\n%s", cmd.String(), err, string(out), errbuf.String())
	}

	r := &MQTTBenchmarkResult{}
	if err := json.Unmarshal(out, r); err != nil {
		tb.Fatalf("Error executing %q: failed to decode output: %v\n\n%s\n\n%s", cmd.String(), err, string(out), errbuf.String())
	}
	return r
}
