package lock

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck        kvtest.IKVClerk
	lockKey   string
	lockValue string
	version   rpc.Tversion
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	lk.lockKey = l
	lk.lockValue = kvtest.RandValue(8)
	return lk
}

func (lk *Lock) Acquire() {
	for {
		_, version, err := lk.ck.Get(lk.lockKey)
		if err != rpc.OK {
			putErr := lk.ck.Put(lk.lockKey, lk.lockValue, 0)
			if putErr == rpc.OK {
				break
			}
		} else {
			if version%2 == 0 {
				putErr := lk.ck.Put(lk.lockKey, lk.lockValue, version)
				if putErr == rpc.OK {
					break
				}
			}
		}
	}
}

func (lk *Lock) Release() {
	_, version, _ := lk.ck.Get(lk.lockKey)
	lk.ck.Put(lk.lockKey, lk.lockValue, version)
}
