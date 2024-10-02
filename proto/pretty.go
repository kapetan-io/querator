package proto

// TODO: Remove
// PPStats pretty prints the stats response
//func PPStats(stats *QueuePartitionStats) string {
//	var buf strings.Builder
//	buf.WriteString("Stats {")
//	_, _ = fmt.Fprintf(&buf, " Total: %d", stats.Total)
//	_, _ = fmt.Fprintf(&buf, " TotalReserved: %d", stats.TotalReserved)
//	_, _ = fmt.Fprintf(&buf, " AverageAge: %s", stats.AverageAge)
//	_, _ = fmt.Fprintf(&buf, " AverageReservedAge: %s", stats.AverageReservedAge)
//	_, _ = fmt.Fprintf(&buf, " ProduceWaiting: %d", stats.ProduceWaiting)
//	_, _ = fmt.Fprintf(&buf, " ReserveWaiting: %d", stats.ReserveWaiting)
//	_, _ = fmt.Fprintf(&buf, " CompleteWaiting: %d", stats.CompleteWaiting)
//	buf.WriteString("}")
//	return buf.String()
//}
