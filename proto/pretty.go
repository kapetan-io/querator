package proto

// TODO: Remove
// PPStats pretty prints the stats response
// func PPStats(stats *QueuePartitionStats) string {
// 	var buf strings.Builder
// 	buf.WriteString("Stats {")
// 	_, _ = fmt.Fprintf(&buf, " Total: %d", stats.Total)
// 	_, _ = fmt.Fprintf(&buf, " NumLeased: %d", stats.NumLeased)
// 	_, _ = fmt.Fprintf(&buf, " AverageAge: %s", stats.AverageAge)
// 	_, _ = fmt.Fprintf(&buf, " AverageLeasedAge: %s", stats.AverageLeasedAge)
// 	_, _ = fmt.Fprintf(&buf, " ProduceWaiting: %d", stats.ProduceWaiting)
// 	_, _ = fmt.Fprintf(&buf, " LeaseWaiting: %d", stats.LeaseWaiting)
// 	_, _ = fmt.Fprintf(&buf, " CompleteWaiting: %d", stats.CompleteWaiting)
// 	buf.WriteString("}")
// 	return buf.String()
// }
