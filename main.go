package main

import (
	"errors"
	"flag"
	"fmt"
	"net/url"
	"strings"
	"time"

	stan "github.com/nats-io/go-nats-streaming"
)

const (
	clientID = "stansync"
)

var (
	srcFlag = flag.String("src", "nats://localhost:4222/test-cluster/channel1", "NATS Streaming url to copy data from")
	dstFlag = flag.String("dst", "", "NATS Streaming url to copy data to")
)

func main() {
	flag.Parse()

	sUrl, sCluster, sChannel := parseNatsUrl(*srcFlag)
	sSc, err := stan.Connect(sCluster, clientID, stan.NatsURL(sUrl))
	check(err)
	defer sSc.Close()

	dUrl, dCluster, dChannel := parseNatsUrl(*dstFlag)
	dSc, err := stan.Connect(dCluster, clientID, stan.NatsURL(dUrl))
	check(err)
	defer dSc.Close()

	sSeq, err := lastSeq(sSc, sChannel)
	if err != nil {
		panic(fmt.Sprintf("Src: %v", err))
	}
	fmt.Println("last src seq", sSeq)
	dSeq, err := lastSeq(dSc, dChannel)
	if err != nil {
		dSeq = 0
	}
	fmt.Println("last dst seq", dSeq)
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func parseNatsUrl(fullurl string) (natsUrl, cluster, channel string) {
	u, err := url.Parse(fullurl)
	check(err)
	paths := strings.Split(u.Path, "/")
	cluster = paths[1]
	channel = paths[2]

	u.Path = ""
	natsUrl = u.String()

	return
}

func lastSeq(sc stan.Conn, channel string) (n uint64, err error) {
	done := make(chan bool)

	var sub stan.Subscription
	sub, err = sc.Subscribe(channel, func(m *stan.Msg) {
		n = m.Sequence
		sub.Unsubscribe()
		close(done)
	}, stan.StartWithLastReceived())
	check(err)

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		err = errors.New("Timeout: Could not detect sequence range")
	}

	return
}
