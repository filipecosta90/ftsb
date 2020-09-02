package main

import (
	"encoding/csv"
	"fmt"
	"github.com/RediSearch/ftsb/benchmark_runner"
	"github.com/mediocregopher/radix/v3"
	"log"
	"strings"
	"sync"
	"time"
)

type processor struct {
	rows           chan string
	cmdChan        chan benchmark_runner.Stat
	wg             *sync.WaitGroup
	vanillaClient  *radix.Pool
	vanillaCluster *radix.Cluster
}

func (p *processor) Init(workerNumber int, _ bool, totalWorkers int) {
	var err error = nil
	if clusterMode {
		poolFunc := func(network, addr string) (radix.Client, error) {
			return radix.NewPool(network, addr, 1, radix.PoolPipelineWindow(time.Duration(0), 0))
		}
		p.vanillaCluster, err = radix.NewCluster([]string{host}, radix.ClusterPoolFunc(poolFunc))
		if err != nil {
			log.Fatalf("Error preparing for redisearch ingestion, while creating new cluster connection. error = %v", err)
		}
	} else {
		p.vanillaClient, err = radix.NewPool("tcp", host, 1, radix.PoolPipelineWindow(0, 0))
		if err != nil {
			log.Fatalf("Error preparing for redisearch ingestion, while creating new pool. error = %v", err)
		}
	}
}

func connectionProcessor(p *processor) {
	cmdSlots := make([][]radix.CmdAction, 0, 0)
	timesSlots := make([][]time.Time, 0, 0)
	slot := 0
	if !clusterMode {
		cmdSlots = append(cmdSlots, make([]radix.CmdAction, 0, 0) )
		timesSlots = append(timesSlots, make([]time.Time, 0, 0) )
	}
	for row := range p.rows {
		cmdType, cmdQueryId, cmd, docFields, bytelen, err := preProcessCmd(row)
		if err == nil {
			cmdSlots[slot], timesSlots[slot] = sendFlatCmd(p, cmdType, cmdQueryId, cmd, docFields, bytelen, 1, cmdSlots[slot], timesSlots[slot])
		}
	}

	p.wg.Done()
}

func getRxLen(v interface{}) (res uint64) {
	res = 0
	switch x := v.(type) {
	case []string:
		for _, i := range x {
			res += uint64(len(i))
		}
	case string:
		res += uint64(len(x))
	default:
		res = 0
	}
	return
}

func sendFlatCmd(p *processor, cmdType, cmdQueryId, cmd string, docfields []string, txBytesCount, insertCount uint64, cmds []radix.CmdAction, times []time.Time ) ([]radix.CmdAction, []time.Time) {
	var err error = nil
	var rcv interface{}
	rxBytesCount := uint64(0)
	var radixFlatCmd = radix.FlatCmd(nil, cmd, docfields[0], docfields[1:])
	cmds = append(cmds, radixFlatCmd)
	start := time.Now()
	times = append(times, start)
	cmds, times = sendIfRequired(p, cmdType, cmdQueryId, cmds, err, times, rxBytesCount, rcv, txBytesCount)
	return cmds, times
}

func sendIfRequired(p *processor, cmdType string, cmdQueryId string, cmds []radix.CmdAction, err error, times []time.Time, rxBytesCount uint64, rcv interface{}, txBytesCount uint64) ([]radix.CmdAction, []time.Time) {
	if len(cmds) >= pipeline {
		err = p.vanillaClient.Do(radix.Pipeline(cmds...))
		endT := time.Now()
		if err != nil {
			if continueOnErr {
				if debug > 0 {
					log.Println(fmt.Sprintf("Received an error with the following command(s): %v, error: %v", cmds, err))
				}
			} else {
				log.Fatal(err)
			}
		}
		for _, t := range times {
			duration := endT.Sub(t)
			took := uint64(duration.Microseconds())
			rxBytesCount += getRxLen(rcv)
			stat := benchmark_runner.NewStat().AddEntry([]byte(cmdType), []byte(cmdQueryId), took, false, false, txBytesCount, rxBytesCount)
			p.cmdChan <- *stat
		}
		cmds = nil
		cmds = make([]radix.CmdAction, 0, 0)
		times = nil
		times = make([]time.Time, 0, 0)
	}
	return cmds, times
}

// ProcessBatch reads eventsBatches which contain rows of databuild for FT.ADD redis command string
func (p *processor) ProcessBatch(b benchmark_runner.Batch, doLoad bool) (outstat benchmark_runner.Stat) {
	outstat = *benchmark_runner.NewStat()
	events := b.(*eventsBatch)
	rowCnt := uint64(len(events.rows))
	if doLoad {
		buflen := rowCnt + 1

		p.cmdChan = make(chan benchmark_runner.Stat, buflen)
		p.wg = &sync.WaitGroup{}
		p.rows = make(chan string, buflen)
		p.wg.Add(1)
		go connectionProcessor(p)
		for _, row := range events.rows {
			p.rows <- row
		}
		close(p.rows)
		p.wg.Wait()

		close(p.cmdChan)

		for cmdStat := range p.cmdChan {
			outstat.Merge(cmdStat)
		}
	}
	events.rows = events.rows[:0]
	ePool.Put(events)
	return
}

func (p *processor) Close(_ bool) {
}

func preProcessCmd(row string) (cmdType string, cmdQueryId string, cmd string, args []string, bytelen uint64, err error) {

	reader := csv.NewReader(strings.NewReader(row))
	argsStr, err := reader.Read()
	if err != nil {
		return
	}

	// we need at least the cmdType and command
	if len(argsStr) >= 3 {
		cmdType = argsStr[0]
		cmdQueryId = argsStr[1]
		cmd = argsStr[2]
		if len(argsStr) > 3 {
			args = argsStr[3:]
		}
		bytelen = uint64(len(row)) - uint64(len(cmdType))
	} else {
		err = fmt.Errorf("input string does not have the minimum required size of 2: %s", row)
	}

	return
}
