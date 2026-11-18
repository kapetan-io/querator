package internal

import (
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/tackle/clock"
)

type requestInbox chan *Request

type shutdownRequests chan *types.ShutdownRequest

type readySignal chan struct{}

type lastUsedUpdates chan lastUsedUpdate

type stopSignal chan struct{}

type requestTimeoutAlarm <-chan clock.Time
