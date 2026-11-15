// Package dht implements the bittorrent dht protocol. For more information
// see http://www.bittorrent.org/beps/bep_0005.html.
package dht

import (
	"context"
	"encoding/hex"
	"errors"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/NoOneTheOne/dht/internal/config"
	"github.com/NoOneTheOne/dht/internal/logging"
)

var udpBufPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 8192)
		return &b
	},
}

var (
	// ErrNotReady is the error when DHT is not initialized.
	ErrNotReady = errors.New("dht is not ready")
	// ErrOnGetPeersResponseNotSet is the error that config
	// OnGetPeersResponseNotSet is not set when call dht.GetPeers.
	ErrOnGetPeersResponseNotSet = errors.New("OnGetPeersResponse is not set")
)

// InitParam represents to configure of dht.
type InitParam struct {
	// in mainline dht, k = 8
	K int
	// for crawling mode, we put all nodes in one bucket, so KBucketSize may
	// not be K
	KBucketSize int
	// candidates are udp, udp4, udp6
	Network string
	// format is `ip:port`
	Address string
	// the prime nodes through which we can join in dht network
	PrimeNodes []string
	// the KBucket expired duration
	KBucketExpiredAfter time.Duration
	// the node expired duration
	NodeExpireAfter time.Duration
	// how long it checks whether the bucket is expired
	CheckKBucketPeriod time.Duration
	// peer token expired duration
	TokenExpiredAfter time.Duration
	// the max transaction id
	MaxTransactionCursor uint64
	// how many nodes routing table can hold
	MaxNodes int
	// callback when got get_peers request
	OnGetPeers func(string, string, int)
	// OnGetPeersResponse func(string, *Peer)
	OnGetPeersResponse func(string, *Peer)
	// callback when got announce_peer request
	OnAnnouncePeer func(string, string, int)
	// blocked ips
	BlockedIPs []string
	// blacklist size
	BlackListMaxSize int
	// StandardMode or CrawlMode
	Mode string
	// the times it tries when send fails
	Try int
	// the size of packet need to be dealt with
	PacketJobLimit int
	// the size of packet handler
	PacketWorkerLimit int
	// the nodes num to be fresh in a KBucket
	RefreshNodeNum   int
	QueryWorkerLimit int
	QueryTimeout     time.Duration
}

// NewStandardConfig returns a InitParam pointer with default values.
func NewStandardConfig() *InitParam {
	return &InitParam{
		K:                    8,
		KBucketSize:          8,
		Network:              "udp4",
		Address:              ":6881",
		PrimeNodes:           config.GetConfig().Tracker.List,
		NodeExpireAfter:      time.Minute * 15,
		KBucketExpiredAfter:  time.Minute * 15,
		CheckKBucketPeriod:   time.Second * 30,
		TokenExpiredAfter:    time.Minute * 10,
		MaxTransactionCursor: math.MaxUint32,
		MaxNodes:             5000,
		BlockedIPs:           make([]string, 0),
		BlackListMaxSize:     65536,
		Try:                  2,
		QueryWorkerLimit:     64,
		QueryTimeout:         5 * time.Second,
		Mode:                 "StandardMode",
		PacketJobLimit:       1024,
		PacketWorkerLimit:    256,
		RefreshNodeNum:       8,
	}
}

// NewCrawlConfig returns a config in crawling mode.
func NewCrawlConfig() *InitParam {
	return &InitParam{
		K:                    8,
		KBucketSize:          math.MaxInt32,
		Network:              "udp4",
		Address:              ":6881",
		PrimeNodes:           config.GetConfig().Tracker.List,
		NodeExpireAfter:      0,
		KBucketExpiredAfter:  0,
		CheckKBucketPeriod:   time.Second * 5,
		TokenExpiredAfter:    time.Minute * 10,
		MaxTransactionCursor: math.MaxUint32,
		MaxNodes:             5000,
		BlockedIPs:           make([]string, 0),
		BlackListMaxSize:     65536,
		Try:                  2,
		QueryWorkerLimit:     64,
		QueryTimeout:         5 * time.Second,
		Mode:                 "CrawlMode",
		PacketJobLimit:       1024,
		PacketWorkerLimit:    256,
		RefreshNodeNum:       64,
	}
}

// DHT represents a DHT node.
type DHT struct {
	*InitParam
	node               *node
	conn               *net.UDPConn
	routingTable       *routingTable
	transactionManager *transactionManager
	peersManager       *peersManager
	tokenManager       *tokenManager
	blackList          *blackList
	bootstrapMgr       *bootstrapManager
	reachability       *reachabilityCheck
	ready              atomic.Bool
	packets            chan packet
	workerTokens       chan struct{}
	stopOnce           sync.Once
	runOnce            sync.Once
	stop               chan struct{}
	stopped            chan struct{}
	background         sync.WaitGroup
	packetWorkers      sync.WaitGroup
	listenerDone       chan struct{}
	ctx                context.Context
	cancel             context.CancelFunc
}

// New returns a DHT pointer. If config is nil, then config will be set to
// the default config.
func New(initParam *InitParam) *DHT {
	// Generate BEP-42 compliant node ID based on local IP
	localIP, err := getLocalIP()
	if err != nil {
		localIP = net.ParseIP("127.0.0.1")
	}
	nodeID := generateNodeID(localIP)

	node, err := newNode(nodeID, initParam.Network, initParam.Address)
	if err != nil {
		logging.Fatalln(err)
	}

	d := &DHT{
		InitParam:    initParam,
		node:         node,
		blackList:    newBlackList(initParam.BlackListMaxSize),
		packets:      make(chan packet, initParam.PacketJobLimit),
		workerTokens: make(chan struct{}, initParam.PacketWorkerLimit),
		stop:         make(chan struct{}),
		stopped:      make(chan struct{}),
	}

	for _, ip := range initParam.BlockedIPs {
		d.blackList.insert(ip, -1)
	}

	go func() {
		for _, ip := range getLocalIPs() {
			d.blackList.insert(ip, -1)
		}

		ip, err := getRemoteIP()
		if err == nil && ip != "" {
			d.blackList.insert(ip, -1)
		}
	}()

	return d
}

// IsStandardMode returns whether mode is StandardMode.
func (dht *DHT) IsStandardMode() bool {
	return dht.Mode == "StandardMode"
}

// IsCrawlMode returns whether mode is CrawlMode.
func (dht *DHT) IsCrawlMode() bool {
	return dht.Mode == "CrawlMode"
}

// init initializes global variables.
func (dht *DHT) init() {
	listener, err := net.ListenPacket(dht.Network, dht.Address)
	if err != nil {
		logging.Fatalln(err)
	}

	dht.conn = listener.(*net.UDPConn)
	dht.routingTable = newRoutingTable(dht.KBucketSize, dht)
	dht.peersManager = newPeersManager(dht)
	dht.tokenManager = newTokenManager(dht.TokenExpiredAfter, dht)
	dht.transactionManager = newTransactionManager(dht.MaxTransactionCursor, dht)
	dht.bootstrapMgr = newBootstrapManager(dht)
	dht.reachability = newReachabilityCheck()

	dht.background.Go(dht.transactionManager.run)
	dht.background.Go(dht.tokenManager.clear)
	dht.background.Go(func() { dht.blackList.clear(dht.stop) })
}

// join makes current node join the dht network.
func (dht *DHT) join() {
	if dht.bootstrapMgr != nil {
		dht.bootstrapMgr.join()
		return
	}

	// Fallback to original implementation if bootstrap manager not initialized
	for _, addr := range dht.PrimeNodes {
		RealAddress, err := net.ResolveUDPAddr(dht.Network, addr)
		if err != nil {
			continue
		}

		// NOTE: Temporary node has NOT node id.
		dht.transactionManager.findNode(
			&node{addr: RealAddress},
			dht.node.id.RawString(),
		)
	}
}

// listen receives message from udp.
func (dht *DHT) listen() {
	dht.listenerDone = make(chan struct{})
	go func() {
		defer close(dht.listenerDone)
		for {
			bufp := udpBufPool.Get().(*[]byte)
			buff := *bufp
			n, RealAddress, err := dht.conn.ReadFromUDP(buff)
			if err != nil {
				udpBufPool.Put(bufp)
				select {
				case <-dht.stop:
					return
				default:
					continue
				}
			}

			RecordPacketReceived()

			data := make([]byte, n)
			copy(data, buff[:n])
			udpBufPool.Put(bufp)
			select {
			case dht.packets <- packet{data, RealAddress}:
			case <-dht.stop:
				return
			default:
				RecordPacketDropped()
				RecordProtocolEvent("packet_queue", "full")
			}
		}
	}()
}

// id returns id near to target if target is not null, otherwise it returns
// the dht node id.
func (dht *DHT) id(target string) string {
	if dht.IsStandardMode() || target == "" {
		return dht.node.id.RawString()
	}
	return target[:15] + dht.node.id.RawString()[15:]
}

// GetPeers returns peers who have announced having infoHash.
func (dht *DHT) GetPeers(infoHash string) error {
	if !dht.ready.Load() || dht.routingTable.Len() == 0 {
		return ErrNotReady
	}

	if dht.OnGetPeersResponse == nil {
		return ErrOnGetPeersResponseNotSet
	}

	if len(infoHash) == 40 {
		data, err := hex.DecodeString(infoHash)
		if err != nil {
			return err
		}
		infoHash = string(data)
	}

	if len(infoHash) != 20 {
		return errors.New("info hash must be 20 bytes or 40 hexadecimal characters")
	}

	neighbors := dht.routingTable.GetNeighbors(
		newBitmapFromString(infoHash), dht.K)

	for _, no := range neighbors {
		dht.transactionManager.getPeers(no, infoHash)
	}

	return nil
}

// Run starts the dht.
func (dht *DHT) Run() {
	dht.RunContext(context.Background())
}

// RunContext starts the dht until ctx is canceled.
func (dht *DHT) RunContext(ctx context.Context) {
	started := false
	dht.runOnce.Do(func() { started = true })
	if !started {
		return
	}
	select {
	case <-dht.stop:
		close(dht.stopped)
		return
	default:
	}
	dht.ctx, dht.cancel = context.WithCancel(ctx)
	dht.init()
	dht.listen()
	dht.background.Go(func() { dht.reachability.logStartupContext(dht.ctx, dht.conn.LocalAddr().(*net.UDPAddr).Port) })
	dht.ready.Store(true)
	dht.background.Go(dht.join)

	var pkt packet
	kBucketTicker := time.NewTicker(dht.CheckKBucketPeriod)
	defer kBucketTicker.Stop()
	evictTicker := time.NewTicker(10 * time.Minute)
	defer evictTicker.Stop()
	defer func() {
		dht.closeResources()
		<-dht.listenerDone
		dht.packetWorkers.Wait()
		dht.background.Wait()
		close(dht.stopped)
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-dht.stop:
			return
		case pkt = <-dht.packets:
			handle(dht, pkt)
		case <-kBucketTicker.C:
			// Use bootstrap manager to check if re-bootstrap is needed
			if dht.bootstrapMgr.needsRebootstrap() {
				dht.background.Go(dht.join)
			} else if dht.transactionManager.len() == 0 {
				// Use optimized refresh instead of regular Fresh
				dht.background.Go(dht.routingTable.OptimizedRefresh)
			}
		case <-evictTicker.C:
			// Periodically evict stale nodes (older than 15 minutes)
			dht.background.Go(func() { dht.routingTable.EvictStaleNodes(15 * time.Minute) })
		}
	}
}

func (dht *DHT) closeResources() {
	dht.cancel()
	dht.ready.Store(false)
	dht.stopOnce.Do(func() { close(dht.stop) })
	if dht.conn != nil {
		_ = dht.conn.Close()
	}
}

// Stop stops the DHT and closes its UDP listener.
func (dht *DHT) Stop() {
	dht.stopOnce.Do(func() { close(dht.stop) })
	dht.runOnce.Do(func() { close(dht.stopped) })
	<-dht.stopped
}

// DHTStats holds DHT statistics for external monitoring
type DHTStats struct {
	RoutingTableNodes  int
	BootstrapCount     int
	ActiveTransactions int
	BlacklistedNodes   int
	PacketQueueDepth   int
	QueryQueueDepth    int
	PacketWorkers      int
}

// GetDHTStats returns current DHT statistics
func (dht *DHT) GetDHTStats() DHTStats {
	if !dht.ready.Load() {
		return DHTStats{}
	}
	stats := DHTStats{
		RoutingTableNodes:  dht.routingTable.Len(),
		ActiveTransactions: dht.transactionManager.len(),
		BlacklistedNodes:   dht.blackList.len(),
		PacketQueueDepth:   len(dht.packets),
		QueryQueueDepth:    len(dht.transactionManager.queryChan),
		PacketWorkers:      len(dht.workerTokens),
	}
	if dht.bootstrapMgr != nil {
		count, _ := dht.bootstrapMgr.getBootstrapStats()
		stats.BootstrapCount = count
	}
	return stats
}
