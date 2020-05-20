package load

// Processor is a type that processes the work for a loading worker
type Processor interface {
	// Init does per-worker setup needed before receiving databuild
	Init(workerNum int, doLoad bool, totalWorkers int)
	// ProcessBatch handles a single batch of databuild
	ProcessBatch(b Batch, doLoad bool, updateRate, deleteRate float64) (metricCount, rowCount, updateCount, DeleteCount, totalLatency, totalBytes uint64)
}

// ProcessorCloser is a Processor that also needs to close or cleanup afterwards
type ProcessorCloser interface {
	Processor
	// Close cleans up after a Processor
	Close(doLoad bool)
}
