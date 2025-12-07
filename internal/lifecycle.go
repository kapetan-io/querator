package internal

// NOTE: Lifecycle management is now integrated directly into the Logical queue's requestLoop.
// See logical.go for the lifecycle implementation (runLifecycle, runScheduled methods).
// This simplification eliminates N goroutines per queue, reducing complexity while
// maintaining per-partition lifecycle isolation through per-partition timing state.
