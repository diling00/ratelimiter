package ratelimiter

import(
    "time"
    "sync"
    "fmt"
)


//
// Rate limiter:
//
//

type RateLimiter struct {
    PeriodSecs          int64
    CountLimit          int64
    StartTime           time.Time
    TotalCount          int64
    CurrentTimestamp	int64
    Count				int64
    SubmitChan	        chan int
    ReleaseChan	        chan int
    WaitServerDone      *sync.WaitGroup
}

func GetCurrentTimestamp(periodSecs int64) int64 {
    return time.Now().Unix() / periodSecs * periodSecs
}


func (rateLimiter *RateLimiter) getCurrentTimestamp() int64 {
    return GetCurrentTimestamp(rateLimiter.PeriodSecs)
}

func CreateRateLimiter(period time.Duration, countLimit int64) *RateLimiter {
    rateLimiter := &RateLimiter{
        PeriodSecs : int64(period / time.Second),
        CountLimit:countLimit,
        StartTime: time.Now(),
        TotalCount:0,
        Count:0,
        CurrentTimestamp: GetCurrentTimestamp(int64(period / time.Second)),
        SubmitChan: make(chan int, 100),
        ReleaseChan: make(chan int, 100),
        WaitServerDone: &sync.WaitGroup{},
    }

    return rateLimiter
}

func (rateLimiter *RateLimiter) Start() {
    rateLimiter.WaitServerDone.Add(1)
    go rateLimiter.RateLimitRoutine()
}

//
// Rate Limiter routine. Go routine to calculate TPS.
//
func (rateLimiter *RateLimiter) RateLimitRoutine() {

    defer rateLimiter.WaitServerDone.Done()

    ticker := time.NewTicker(time.Millisecond * 5)
    defer ticker.Stop()

    draining := false

    messageLoop:
    for ;; {
        updateCount := 0
        select {
        case count := <-rateLimiter.SubmitChan:
            if count == 0 {
                draining = true
            } else if count < 0 {
                break messageLoop
            } else if !draining {
                updateCount = 1
            }

            break

        case <- ticker.C:
            break
        }

        for {
            timestamp := rateLimiter.getCurrentTimestamp()
            if timestamp == rateLimiter.CurrentTimestamp {
                break
            }

            rateLimiter.CurrentTimestamp = timestamp
            if rateLimiter.Count <= rateLimiter.CountLimit {
                rateLimiter.Count = 0
                if draining {
                    break messageLoop
                }
            } else {
                rateLimiter.Count -= rateLimiter.CountLimit
                clientsToRelease := rateLimiter.Count
                if clientsToRelease > rateLimiter.CountLimit {
                    clientsToRelease = rateLimiter.CountLimit
                }
                for i := int64(0); i < clientsToRelease; i++ {
                    rateLimiter.ReleaseChan <- 1
                }
            }
        }

        if updateCount > 0 {
            rateLimiter.Count ++;
            rateLimiter.TotalCount ++
            if rateLimiter.Count <= rateLimiter.CountLimit {
                rateLimiter.ReleaseChan <- 1
            }
        }
    }

    // unblock all go routines blocked by submit/release channel
    close(rateLimiter.ReleaseChan)
    close(rateLimiter.SubmitChan)
}

func (rateLimiter *RateLimiter) Throttle() {
    rateLimiter.SubmitChan <- 1
    <-rateLimiter.ReleaseChan
    //fmt.Printf("total requests = %d\n",rateLimiter.TotalCount)
}

func (rateLimiter *RateLimiter) Shutdown() {
    rateLimiter.SubmitChan <- 0
    rateLimiter.WaitServerDone.Wait()
    rateLimiter.Report()
}

func (rateLimiter *RateLimiter) Terminate() {
    rateLimiter.SubmitChan <- -1
    rateLimiter.WaitServerDone.Wait()
    rateLimiter.Report()
}

func (rateLimiter *RateLimiter) Report() {
    fmt.Printf("total requests = %d\n",rateLimiter.TotalCount)
    fmt.Printf("overall TPS = %f\n", float64(rateLimiter.TotalCount) / float64(time.Since(rateLimiter.StartTime)/ time.Second))
}
