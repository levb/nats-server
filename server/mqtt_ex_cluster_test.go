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
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
)

type testMQTTTarget struct {
	name string
	dial []string
}

func TestMQTTExCompliance(t *testing.T) {
	if mqttCLICommandPath == "" {
		t.Skip(`"mqtt" command is not found in $PATH nor $MQTT_CLI. See https://hivemq.github.io/mqtt-cli/docs/installation/#debian-package for installation instructions`)
	}

	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	o = s.getOpts()
	defer testMQTTShutdownServer(s)

	cmd := exec.Command(mqttCLICommandPath, "test", "-V", "3", "-p", strconv.Itoa(o.MQTT.Port))

	output, err := cmd.CombinedOutput()
	t.Log(string(output))
	if err != nil {
		if exitError, ok := err.(*exec.ExitError); ok {
			t.Fatalf("mqtt cli exited with error: %v", exitError)
		}
	}
}

func TestMQTTExRetainedMessages(t *testing.T) {
	if mqttTestCommandPath == "" {
		t.Skip(`"mqtt-test" command is not found in $PATH.`)
	}

	for _, topo := range []struct {
		name  string
		makef func(t *testing.T) ([]testMQTTTarget, func())
	}{
		// {
		// 	name:  "single server",
		// 	makef: testMQTTmakeSingleServer,
		// },
		{
			name:  "server with leafnode",
			makef: testMQTTmakeServerWithLeafnode("HUBD", "LEAFD", true),
		},
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

			// t.Run("init at "+targets[0].name, func(t *testing.T) {
			// 	mqttInitServer(t, targets[0].dial[0])
			// })

			for _, target := range targets {
				t.Run(target.name, func(t *testing.T) {
					// mqttRunTestCommand(t, "subret", target.dial,
					// 	"--qos", "0",
					// 	"--n", "1", // number of subscribe requests
					// 	"--num-topics", "1",
					// 	"--size", "100",
					// )
					i := 0
					dial := target.dial[i%len(target.dial)]
					i++
					u, p, h, port := parsedial(dial)

					cmd := exec.Command(mqttCLICommandPath, "publish",
						"-d",
						"-V", "3",
						"--user", u,
						"--password", p,
						"--host", h,
						"--port", port,
						"--message", "hello",
						"--topic", "foo",
						"--retain",
					)
					t.Log(cmd.String())

					output, err := cmd.CombinedOutput()
					t.Log(string(output))
					if err != nil {
						if exitError, ok := err.(*exec.ExitError); ok {
							t.Fatalf("mqtt cli exited with error: %v", exitError)
						}
					}

					dial = target.dial[i%len(target.dial)]
					i++
					u, p, h, port = parsedial(dial)

					cmd = exec.Command(mqttCLICommandPath, "subscribe",
						"-V", "3",
						"--user", u,
						"--password", p,
						"--host", h,
						"--port", port,
						"--topic", "foo",
					)
					t.Log(cmd.String())

					output, err = cmd.CombinedOutput()
					t.Log(string(output))
					if err != nil {
						if exitError, ok := err.(*exec.ExitError); ok {
							t.Fatalf("mqtt cli exited with error: %v", exitError)
						}
					}

					if !strings.HasSuffix(string(output), "hello") {
						t.Fatalf("expected retained message to be received")
					}
				})
			}
		})
	}
}

func TestMQTTRetainedMessageRecoveredOnRestart(t *testing.T) {
	if mqttCLICommandPath == "" {
		t.Skip(`"mqtt" command is not found in $PATH.`)
	}

	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	o = s.getOpts()

}

func testMQTTmakeSingleServer(t *testing.T) ([]testMQTTTarget, func()) {
	t.Helper()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	o = s.getOpts()
	addr := fmt.Sprintf("%s:%d", o.Host, o.MQTT.Port)

	return []testMQTTTarget{{name: s.info.Name, dial: []string{addr}}},
		func() { testMQTTShutdownServer(s) }
}

func testMQTTmakeServerWithLeafnode(hubd, leafd string, connectSystemAccount bool) func(t *testing.T) ([]testMQTTTarget, func()) {
	return func(t *testing.T) ([]testMQTTTarget, func()) {
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

		sysRemote := ""
		if connectSystemAccount {
			sysRemote = `{ url: "nats://admin:s3cr3t!@` + leafRemoteAddr + `", account: "$SYS" },` + "\n\t\t"
		}
		if leafd != "" {
			leafd = "domain: " + leafd + ", "
		}
		leafconf := `
listen: 127.0.0.1:-1

server_name: LEAF
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
		leafs, leafo := RunServerWithConfig(leafConf)

		var targets []testMQTTTarget
		targets = append(targets, testMQTTTarget{
			name: "server",
			dial: []string{
				fmt.Sprintf("one:p@%s:%d", o.MQTT.Host, o.MQTT.Port),
			},
		})
		targets = append(targets, testMQTTTarget{
			name: "leaf",
			dial: []string{
				fmt.Sprintf("one:p@%s:%d", leafo.MQTT.Host, leafo.MQTT.Port),
			},
		})
		targets = append(targets, testMQTTTarget{
			name: "server to leaf",
			dial: []string{
				fmt.Sprintf("one:p@%s:%d", o.MQTT.Host, o.MQTT.Port),
				fmt.Sprintf("one:p@%s:%d", leafo.MQTT.Host, leafo.MQTT.Port),
			},
		})
		targets = append(targets, testMQTTTarget{
			name: "leaf to server",
			dial: []string{
				fmt.Sprintf("one:p@%s:%d", leafo.MQTT.Host, leafo.MQTT.Port),
				fmt.Sprintf("one:p@%s:%d", o.MQTT.Host, o.MQTT.Port),
			},
		})

		return targets, func() {
			testMQTTShutdownServer(leafs)
			testMQTTShutdownServer(s)
			os.Remove(serverConf)
			os.Remove(leafConf)
		}
	}
}

func testMQTTmakeCluster(size int, domain string) func(t *testing.T) ([]testMQTTTarget, func()) {
	return func(t *testing.T) ([]testMQTTTarget, func()) {
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

		var targets []testMQTTTarget

		for _, s1 := range cl.servers {
			o1 := s1.getOpts()
			for _, s2 := range cl.servers {
				o2 := s2.getOpts()
				if s1 == s2 {
					targets = append(targets, testMQTTTarget{
						name: s1.info.Name,
						dial: []string{
							fmt.Sprintf("one:p@%s:%d", o1.MQTT.Host, o1.MQTT.Port),
						},
					})
				} else {
					targets = append(targets, testMQTTTarget{
						name: s1.info.Name + " to " + s2.info.Name,
						dial: []string{
							fmt.Sprintf("one:p@%s:%d", o1.MQTT.Host, o1.MQTT.Port),
							fmt.Sprintf("one:p@%s:%d", o2.MQTT.Host, o2.MQTT.Port),
						},
					})
				}
			}
		}
		return targets[1:2], func() { cl.shutdown() }
	}
}

func testMQTTmakeClusterWithLeafnodeCluster(hubd, leafd string, connectSystemAccount bool) func(t *testing.T) ([]testMQTTTarget, func()) {
	return func(t *testing.T) ([]testMQTTTarget, func()) {
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

		var targets []testMQTTTarget

		// // hub.servers[0]
		targets = append(targets, testMQTTTarget{
			name: hub.servers[0].info.Name,
			dial: []string{
				fmt.Sprintf("one:p@%s:%d", hub.opts[0].MQTT.Host, hub.opts[0].MQTT.Port),
			},
		})

		// spoke.servers[0]
		// targets = append(targets, testMQTTTarget{
		// 	name: spoke.servers[0].info.Name,
		// 	dial: []string{
		// 		fmt.Sprintf("one:p@%s:%d", spoke.opts[0].MQTT.Host, spoke.opts[0].MQTT.Port),
		// 	},
		// })

		// publish to hub.servers[1], subscribe at leafCl.servers[2]
		// targets = append(targets, testMQTTTarget{
		// 	name: hub.servers[1].info.Name + " to " + spoke.servers[2].info.Name,
		// 	dial: []string{
		// 		fmt.Sprintf("one:p@%s:%d", hub.opts[1].MQTT.Host, hub.opts[1].MQTT.Port),
		// 		fmt.Sprintf("one:p@%s:%d", spoke.opts[2].MQTT.Host, spoke.opts[2].MQTT.Port),
		// 	},
		// })

		// // publish to leafCl.servers[1], subscribe at hub.servers[2]
		// targets = append(targets, testMQTTTarget{
		// 	name: spoke.servers[1].info.Name + " to " + hub.servers[2].info.Name,
		// 	dial: []string{
		// 		fmt.Sprintf("one:p@%s:%d", spoke.opts[1].MQTT.Host, spoke.opts[1].MQTT.Port),
		// 		fmt.Sprintf("one:p@%s:%d", hub.opts[2].MQTT.Host, hub.opts[2].MQTT.Port),
		// 	},
		// })

		return targets, func() {
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

func parsedial(dial string) (username, password, host, port string) {
	ss := strings.Split(dial, "@")
	if len(ss) > 1 {
		pp := strings.Split(ss[0], ":")
		if len(pp) > 1 {
			password = pp[1]
		}
		username = pp[0]
		dial = ss[1]
	}
	host, port, _ = net.SplitHostPort(dial)
	return username, password, host, port
}
