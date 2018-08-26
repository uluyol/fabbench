package bench

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/uluyol/fabbench/db"
)

// A WorkloadReqFunc executes one request in a workload.
// If wg is not nil, Done() will be called once on wg once the function is
// complete.
type WorkloadReqFunc func(ctx context.Context, args *issueArgs, rng *rand.Rand, wg *sync.WaitGroup, start time.Time)

func ReadReq(ctx context.Context, args *issueArgs, rng *rand.Rand, wg *sync.WaitGroup, start time.Time) {
	if wg != nil {
		defer wg.Done()
	}
	key := args.readKeyGen.Next(rng)
	_, meta, err := args.db.Get(ctx, key)
	latency := time.Since(start)
	args.readC <- resDoneReq(args.tsStep, getHost(meta), latency, err)
}

func WriteReq(ctx context.Context, args *issueArgs, rng *rand.Rand, wg *sync.WaitGroup, start time.Time) {
	if wg != nil {
		defer wg.Done()
	}
	key := args.writeKeyGen.Next(rng)
	val := args.valGen.Next(rng)
	meta, err := args.db.Put(ctx, key, val)
	latency := time.Since(start)
	args.writeC <- resDoneReq(args.tsStep, getHost(meta), latency, err)
}

func getHost(m db.Meta) string {
	if hi, ok := db.GetHostInfo(m); ok {
		return hi.ID()
	}
	return ""
}
