package queues_stream

import pb "github.com/kubemq-io/protobuf/go"

type QueueInfo struct {
	Name          string `json:"name"`
	Messages      int64  `json:"messages"`
	Bytes         int64  `json:"bytes"`
	FirstSequence int64  `json:"first_sequence"`
	LastSequence  int64  `json:"last_sequence"`
	Sent          int64  `json:"sent"`
	Subscribers   int    `json:"subscribers"`
	Waiting       int64  `json:"waiting"`
	Delivered     int64  `json:"delivered"`
}
type QueuesInfo struct {
	TotalQueues int          `json:"total_queues"`
	Sent        int64        `json:"sent"`
	Waiting     int64        `json:"waiting"`
	Delivered   int64        `json:"delivered"`
	Queues      []*QueueInfo `json:"queues"`
}

func fromQueuesInfoPb(info *pb.QueuesInfo) *QueuesInfo {
	q := &QueuesInfo{
		TotalQueues: int(info.TotalQueue),
		Sent:        info.Sent,
		Waiting:     info.Waiting,
		Delivered:   info.Delivered,
		Queues:      nil,
	}
	for _, queue := range info.Queues {
		q.Queues = append(q.Queues, &QueueInfo{
			Name:          queue.Name,
			Messages:      queue.Messages,
			Bytes:         queue.Bytes,
			FirstSequence: queue.FirstSequence,
			LastSequence:  queue.LastSequence,
			Sent:          queue.Sent,
			Subscribers:   int(queue.Subscribers),
			Waiting:       queue.Waiting,
			Delivered:     queue.Delivered,
		})
	}
	return q
}
