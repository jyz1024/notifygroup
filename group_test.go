package notifygroup

import (
	"log"
	"sync"
	"testing"
	"time"
)

func TestDo(t *testing.T) {
	key := "group_key"
	wg := sync.WaitGroup{}
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		time.Sleep(time.Millisecond * 10)
		go func(val int) {
			defer wg.Done()
			ch := make(chan interface{}, 0)
			Do(key, ch, func() interface{} {
				log.Println(key, val)
				time.Sleep(time.Millisecond * 500)
				return true
			})
			<-ch
		}(i)
	}
	wg.Wait()
}
