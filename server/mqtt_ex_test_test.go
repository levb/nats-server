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
		makef func(t *testing.T) ([]mqttexTarget, func())
	}{
		{
			name:  "single server",
			makef: testMQTTmakeSingleServer,
		},
		// {
		// 	name:  "server with leafnode",
		// 	makef: testMQTTmakeServerWithLeafnode("HUBD", "LEAFD", true),
		// },
		// {
		// 	name:  "server with leafnode no domains",
		// 	makef: testMQTTmakeServerWithLeafnode("", "", true),
		// },
		// {
		// 	name:  "server with leafnode no system account",
		// 	makef: testMQTTmakeServerWithLeafnode("HUBD", "LEAFD", false),
		// },
		// {
		// 	name:  "cluster",
		// 	makef: testMQTTmakeCluster(4, ""),
		// },
		// {
		// 	name:  "cluster with leafnode cluster",
		// 	makef: testMQTTmakeClusterWithLeafnodeCluster("HUBD", "LEAFD", true),
		// },
		// {
		// 	name:  "cluster with leafnode cluster no system account",
		// 	makef: testMQTTmakeClusterWithLeafnodeCluster("HUBD", "LEAFD", false),
		// },
		// {
		// 	name:  "supercluster with leafnode cluster",
		// 	makef: testMQTTmakeSuperClusterWithLeafnodeCluster,
		// },
	} {
		t.Run(topo.name, func(t *testing.T) {
			targets, cleanup := topo.makef(t)
			t.Cleanup(cleanup)
			if len(targets) == 0 {
				t.SkipNow()
			}

			numRMS := 10
			strNumRMS := "10"
			for _, target := range targets {
				t.Run(target.name, func(t *testing.T) {
					topic := "subret_" + nuid.Next()

					// publish retained messages, one at a time, round-robin across pubNodes.
					iNode := 0
					for i := 0; i < numRMS; i++ {
						pubTopic := fmt.Sprintf("%s/%d", topic, i)
						pubNode := target.pubNodes[iNode%len(target.pubNodes)]
						mqttexRunTestOnNodes(t, "pub", []mqttexNode{pubNode},
							"--retain",
							"--topic", pubTopic,
							"--qos", "0",
							"--size", "100",
						)
						iNode++
					}

					for _, node := range target.subNodes {
						t.Run(fmt.Sprintf("subscribe at %s", node.Name()), func(t *testing.T) {
							mqttexRunTestOnNodes(t, "sub", []mqttexNode{node},
								"--retained", strNumRMS,
								"--qos", "0",
								"--topic", topic,
							)
						})
					}

					for _, node := range target.subNodes {
						t.Run(fmt.Sprintf("Reload %s", node.Name()), func(t *testing.T) {
							node.Reload(t)
						})
					}

					for _, node := range target.subNodes {
						t.Run(fmt.Sprintf("subscribe again at %s", node.Name()), func(t *testing.T) {
							mqttexRunTestOnNodes(t, "sub", []mqttexNode{node},
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

func testMQTTmakeSingleServer(t *testing.T) ([]mqttexTarget, func()) {
	t.Helper()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	node := mqttexNode{server: s}
	return []mqttexTarget{
			{
				name:     s.info.Name,
				pubNodes: []mqttexNode{node},
				subNodes: []mqttexNode{node},
			},
		},
		func() {
			testMQTTShutdownServer(s)
		}
}

func testMQTTmakeServerWithLeafnode(hubd, leafd string, connectSystemAccount bool) func(t *testing.T) ([]mqttexTarget, func()) {
	return func(t *testing.T) ([]mqttexTarget, func()) {
		t.Helper()

		if hubd != "" {
			hubd = "domain: " + hubd + ", "
		}
		sconf := `
listen: 127.0.0.1:-1

server_name: HUB
jetstream: {max_mem_store: 256MB, max_file_store: 2GB, ` + hubd + `store_dir: '` + t.TempDir() + `'}

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
		serverConf := createConfFile(t, []byte(sconf))
		s, o := RunServerWithConfig(serverConf)
		leafRemoteAddr := fmt.Sprintf("%s:%d", o.LeafNode.Host, o.LeafNode.Port)
		hubNode := mqttexNode{server: s, username: "one", password: "p"}

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
jetstream: {max_mem_store: 256MB, max_file_store: 2GB, ` + leafd + `store_dir: '` + t.TempDir() + `'}

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
		leafConf := createConfFile(t, []byte(leafconf))
		leafs, _ := RunServerWithConfig(leafConf)
		spokeNode := mqttexNode{server: leafs, username: "one", password: "p"}

		var targets []mqttexTarget
		targets = append(targets, mqttexTarget{
			name:     "pub to all",
			pubNodes: []mqttexNode{hubNode, spokeNode},
			subNodes: []mqttexNode{hubNode, spokeNode},
		})
		// targets = append(targets, mqttexTarget{
		// 	name:     "pub to SPOKE",
		// 	pubNodes: []mqttexNode{spokeNode},
		// 	subNodes: []mqttexNode{hubNode, spokeNode},
		// })
		// targets = append(targets, mqttexTarget{
		// 	name:     "pub to HUB",
		// 	pubNodes: []mqttexNode{hubNode},
		// 	subNodes: []mqttexNode{hubNode, spokeNode},
		// })

		return targets, func() {
			testMQTTShutdownServer(leafs)
			testMQTTShutdownServer(s)
			os.Remove(serverConf)
			os.Remove(leafConf)
		}
	}
}

func testMQTTmakeCluster(size int, domain string) func(t *testing.T) ([]mqttexTarget, func()) {
	return func(t *testing.T) ([]mqttexTarget, func()) {
		t.Helper()
		if size < 3 {
			t.Fatal("cluster size must be at least 3")
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
		cl := createJetStreamClusterWithTemplate(t, clusterConf, "MQTT", size)
		cl.waitOnLeader()

		targetPubAll := mqttexTarget{
			name: "publish to one",
		}
		for _, s := range cl.servers {
			targetPubAll.subNodes = append(targetPubAll.subNodes, mqttexNode{server: s, username: "one", password: "p"})
		}
		targetPubAll.pubNodes = targetPubAll.subNodes

		targetPubRandom := mqttexTarget{
			name:     "publish to all",
			pubNodes: []mqttexNode{{server: cl.randomServer(), username: "one", password: "p"}},
			subNodes: targetPubAll.subNodes,
		}

		return []mqttexTarget{targetPubAll, targetPubRandom}, func() { cl.shutdown() }
	}
}

func testMQTTmakeClusterWithLeafnodeCluster(hubd, leafd string, connectSystemAccount bool) func(t *testing.T) ([]mqttexTarget, func()) {
	return func(t *testing.T) ([]mqttexTarget, func()) {
		t.Helper()

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
		hub := createJetStreamClusterWithTemplate(t, hubConf, "HUB", 3)
		hub.waitOnLeader()

		// Pick a host to connect leafnodes to
		lno := hub.randomNonLeader().getOpts().LeafNode
		leafRemoteAddr := fmt.Sprintf("%s:%d", lno.Host, lno.Port)
		hubPubNode := mqttexNode{server: hub.randomNonLeader(), username: "one", password: "p"}
		allNodes := []mqttexNode{}
		for _, s := range hub.servers {
			allNodes = append(allNodes, mqttexNode{server: s, username: "one", password: "p"})
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
		spoke := createJetStreamCluster(t, leafConf, "SPOKE", "SPOKE-", 3, 22111, false)
		expectedConnections := 2
		if !connectSystemAccount {
			expectedConnections = 1
		}
		for _, s := range spoke.servers {
			checkLeafNodeConnectedCount(t, s, expectedConnections)
		}
		spoke.waitOnPeerCount(3)
		spokePubNode := mqttexNode{server: spoke.randomNonLeader(), username: "one", password: "p"}
		for _, s := range spoke.servers {
			allNodes = append(allNodes, mqttexNode{server: s, username: "one", password: "p"})
		}

		return []mqttexTarget{
				{
					name:     "publish to all",
					pubNodes: allNodes,
					subNodes: allNodes,
				},
				{
					name:     "publish to hub",
					pubNodes: []mqttexNode{hubPubNode},
					subNodes: allNodes,
				},
				{
					name:     "publish to spoke",
					pubNodes: []mqttexNode{spokePubNode},
					subNodes: allNodes,
				},
			},
			func() {
				spoke.shutdown()
				hub.shutdown()
			}
	}
}

// func testMQTTmakeSuperClusterWithLeafnodeCluster(t *testing.T) (testMQTTTarget, func()) {
// 	t.Helper()

// 	target := testMQTTTarget{
// 		testName: "super cluster with leafnode cluster",
// 	}

// 	sc := createJetStreamSuperClusterWithTemplateAndModHook(t, jsClusterAccountsTempl, 3, 2,
// 		func(_, _, _, conf string) string {
// 			// set JetStream domain to "HUBDOMAIN"
// 			conf = strings.Replace(conf, "store_dir:", "domain: HUBDOMAIN, store_dir:", 1)
// 			// add MQTT config
// 			conf = conf + "\n" + testMQTTClusterConf
// 			return conf
// 		},
// 		nil)

// 	for _, cl := range sc.clusters {
// 		t.Log("<>/<> cluster", cl.name)
// 		target.servers = testMQTTappendTargetServers(target.servers, cl.servers, cl.name)
// 	}

// 	leafCluster := sc.createLeafNodesWithDomain("LNC", 3, "LEAFDOMAIN")
// 	target.servers = testMQTTappendTargetServers(target.servers, leafCluster.servers, leafCluster.name)

// 	o := sc.clusters[0].servers[0].getOpts()
// 	testMQTTinitJS(t, fmt.Sprintf("%s:%d", o.Host, o.MQTT.Port))

// 	return target, func() {
// 		leafCluster.shutdown()
// 		sc.shutdown()
// 	}
// }

type mqttexNode struct {
	server   *Server
	username string
	password string
}

func (n *mqttexNode) String() string {
	o := n.server.getOpts().MQTT

	switch {
	case n.username != "" && n.password != "":
		return fmt.Sprintf("%s:%s@%s:%d#%s", n.username, n.password, o.Host, o.Port, n.server.Name())
	case n.username != "":
		return fmt.Sprintf("%s@%s:%d#%s", n.username, o.Host, o.Port, n.server.Name())
	default:
		return fmt.Sprintf("%s:%d#%s", o.Host, o.Port, n.server.Name())
	}
}

func (n *mqttexNode) Name() string {
	return n.server.Name()
}

func (n *mqttexNode) Reload(tb testing.TB) {
	s := n.server
	o := s.getOpts()
	s.Shutdown()
	n.server = testMQTTRunServer(tb, o)
}

type mqttexTarget struct {
	name     string
	pubNodes []mqttexNode
	subNodes []mqttexNode
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

func mqttexRunTestOnNodes(tb testing.TB, subCommand string, nodes []mqttexNode, extraArgs ...string) *MQTTBenchmarkResult {
	tb.Helper()

	ss := make([]string, 0, len(nodes))
	for _, n := range nodes {
		ss = append(ss, n.String())
	}
	return mqttexRunTest(tb, subCommand, ss, extraArgs...)
}

func mqttexRunTest(tb testing.TB, subCommand string, servers []string, extraArgs ...string) *MQTTBenchmarkResult {
	tb.Helper()

	if mqttexTestCommandPath == "" {
		tb.Skip(`"mqtt-test" command is not found in $PATH.`)
	}

	args := []string{subCommand /*, "-q" */}
	for _, s := range servers {
		args = append(args, "-s", s)
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

	tb.Logf("<>/<> Running %q", cmd.String())
	// tb.Logf("<>/<> Output of %q:\n%s\n%s", cmd.String(), string(out), errbuf.String())

	r := &MQTTBenchmarkResult{}
	if err := json.Unmarshal(out, r); err != nil {
		tb.Fatalf("Error executing %q: failed to decode output: %v\n\n%s\n\n%s", cmd.String(), err, string(out), errbuf.String())
	}
	return r
}
