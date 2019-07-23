package main

import (
	"bufio"
	"flag"
	"github.com/filipecosta90/ftsb/load"
	"github.com/gomodule/redigo/redis"
	"log"
	"sync"
)

// Program option vars:
var (
	host        string
	connections uint64
	pipeline    uint64
	checkChunks uint64
	singleQueue bool
	dataModel   string
)

// Global vars
var (
	loader *load.BenchmarkRunner
	//bufPool sync.Pool
)

// allows for testing
var fatal = log.Fatal

// Parse args:
func init() {
	loader = load.GetBenchmarkRunnerWithBatchSize(1000)
	flag.StringVar(&host, "host", "localhost:6379", "The host:port for Redis connection")
	flag.Uint64Var(&connections, "connections", 10, "The number of connections per worker")
	flag.Uint64Var(&pipeline, "pipeline", 50, "The pipeline's size")
	flag.Parse()
}

type benchmark struct {
	dbc *dbCreator
}

type RedisIndexer struct {
	partitions uint
}

func (i *RedisIndexer) GetIndex(itemsRead uint64, p *load.Point) int {
	return int(uint(itemsRead) % i.partitions)
}

func (b *benchmark) GetPointDecoder(br *bufio.Reader) load.PointDecoder {
	return &decoder{scanner: bufio.NewScanner(br)}
}

func (b *benchmark) GetBatchFactory() load.BatchFactory {
	return &factory{}
}

func (b *benchmark) GetPointIndexer(maxPartitions uint) load.PointIndexer {
	return &RedisIndexer{partitions: maxPartitions}
}

func (b *benchmark) GetProcessor() load.Processor {
	return &processor{b.dbc, nil, nil, nil}
}

func (b *benchmark) GetDBCreator() load.DBCreator {
	return b.dbc
}

type processor struct {
	dbc     *dbCreator
	rows    chan string
	metrics chan uint64
	wg      *sync.WaitGroup
}

func connectionProcessor(wg *sync.WaitGroup, rows chan string, metrics chan uint64, pool *redis.Pool) {
	conn := pool.Get()
	defer conn.Close()
	for row := range rows {
		metrics <- sendRedisCommand(row, conn)
	}
	conn.Close()
	wg.Done()
}

func (p *processor) Init(_ int, _ bool) {}

// ProcessBatch reads eventsBatches which contain rows of data for FT.ADD redis command string
func (p *processor) ProcessBatch(b load.Batch, doLoad bool) (uint64, uint64) {
	events := b.(*eventsBatch)
	rowCnt := uint64(len(events.rows))
	metricCnt := uint64(0)
	if doLoad {
		buflen := rowCnt + 1
		p.metrics = make(chan uint64, buflen)
		p.wg = &sync.WaitGroup{}
		p.rows = make(chan string, buflen)
		p.wg.Add(1)
		go connectionProcessor(p.wg, p.rows, p.metrics, p.dbc.pool)
		for _, row := range events.rows {
			p.rows <- row
		}
		close(p.rows)
		p.wg.Wait()
		close(p.metrics)

		for val := range p.metrics {
			metricCnt += val
		}
	}
	events.rows = events.rows[:0]
	ePool.Put(events)
	return metricCnt, rowCnt
}

func (p *processor) Close(_ bool) {
}

func main() {
	workQueues := uint(load.WorkerPerQueue)
	loader.RunBenchmark(&benchmark{dbc: &dbCreator{}}, workQueues)
}
