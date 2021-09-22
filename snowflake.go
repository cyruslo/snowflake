package snowflake

import (
	"github.com/golang/glog"
	"sync"
	"time"
)

// 0|0000000 00000000 00000000 00000000 00000000 00|000000 0000|0000 00000000
// *\--------------timestampBits(41)--------------/\-nodeBits-/\--sequence--/
// nodeIdBits:
// 000|000 0000
// dataCenter|nodeBits
const (
	// 设置起始时间(时间戳/毫秒)：2021-01-01 00:00:00，有效期69年
	epoch = int64(1609430400000)
	// 时间戳占用位数
	timestampBits = uint(41)
	// 数据中心id所占位数
	dataCenterBits = uint(3)
	// node所占位数
	nodeBits = uint(7)
	// 序列所占位数
	sequenceBits = uint(12)
	// 时间戳最大值
	timestampMax = int64(-1 ^ (-1 << timestampBits))
	// 数据中心id最大值
	dataCenterMax = int64(-1 ^ (-1 << dataCenterBits))
	// node最大值
	nodeMax = int64(-1 ^ (-1 << nodeBits))
	// 序列号最大值
	sequenceMax = int64(-1 ^ (-1 << sequenceBits))
	// 时间戳位置左移位数
	timestampShift = sequenceBits + nodeBits + dataCenterBits
	// 数据中心id位置左移位数
	dataCenterShift = sequenceBits + nodeBits
	// node位置左移位数
	nodeShift = sequenceBits
)

type snowflake struct {
	sync.Mutex         // 锁
	timestamp    int64 // 时间戳 ，毫秒
	nodeId       int64 // 工作节点
	dataCenterId int64 // 数据中心机房id
	sequence     int64 // 序列号
}

func (s *snowflake) NextVal() int64 {
	s.Lock()
	now := time.Now().UnixNano() / 1000000 // 转毫秒
	if s.timestamp == now {
		// 当同一时间戳（精度：毫秒）下多次生成id会增加序列号
		s.sequence = (s.sequence + 1) & sequenceMax
		if s.sequence == 0 {
			// 如果当前序列超出12bit长度，则需要等待下一毫秒
			// 下一毫秒将使用sequence:0
			for now <= s.timestamp {
				now = time.Now().UnixNano() / 1000000
			}
		}
	} else {
		// 不同时间戳（精度：毫秒）下直接使用序列号：0
		s.sequence = 0
	}
	t := now - epoch
	if t > timestampMax {
		s.Unlock()
		glog.Errorf("epoch must be between 0 and %d", timestampMax-1)
		return 0
	}
	s.timestamp = now
	r := (t)<<timestampShift | (s.dataCenterId << dataCenterShift) | (s.nodeId << nodeShift) | s.sequence
	s.Unlock()
	return r
}
