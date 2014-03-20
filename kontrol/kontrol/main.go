package main

import (
	"flag"
	"strings"

	"github.com/koding/kite/config"
	"github.com/koding/kite/kontrol"
)

func main() {
	var (
		ip           = flag.String("ip", "0.0.0.0", "")
		port         = flag.Int("port", 4000, "")
		etcdAddr     = flag.String("etcd-addr", "http://127.0.0.1:4001", "The public host:port used for etcd server.")
		etcdBindAddr = flag.String("etcd-bind-addr", ":4001", "The listening host:port used for etcd server.")
		peerAddr     = flag.String("peer-addr", "http://127.0.0.1:7001", "The public host:port used for peer communication.")
		peerBindAddr = flag.String("peer-bind-addr", ":7001", "The listening host:port used for peer communication.")
		name         = flag.String("name", "", "name of the instance")
		dataDir      = flag.String("data-dir", "", "directory to store data")
		peers        = flag.String("peers", "", "comma seperated peer addresses")
	)

	flag.Parse()

	conf := config.MustGet()
	conf.IP = *ip
	conf.Port = *port

	k := kontrol.New(conf)
	k.EtcdAddr = *etcdAddr
	k.EtcdBindAddr = *etcdBindAddr
	k.PeerAddr = *peerAddr
	k.PeerBindAddr = *peerBindAddr

	if *name != "" {
		k.Name = *name
	}
	if *dataDir != "" {
		k.DataDir = *dataDir
	}
	if *peers != "" {
		k.Peers = strings.Split(*peers, ",")
	}

	k.Run()
}
