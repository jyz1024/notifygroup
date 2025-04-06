package notifygroup

import (
	"sync"
)

// 适用场景：并发场景&fun执行一次和执行多次没有区别，只需要保证调用之后方法会执行一次；
// 作用：对于相同key并发提交的一批任务，保证传入的function会在"最后一次提交任务后"执行一次。1.减少fun的调用次数 2.串行执行，避免并发场景下执行顺序改变

type Group struct {
	// l map锁
	l sync.Mutex
	// wait 等待批量执行的队列（队列内的所有任务只会执行一次fun方法）
	wait map[string][]chan<- interface{}
	// signal 执行中信号，不同批次任务强制串行执行（有无当前key的任务正在执行）
	// key存在且chan写入阻塞时，表示有任务正在执行，此时如果有相同key的任务提交，都会进入wait队列内
	signal map[string]*signal
}

type signal struct {
	taken int
	ch    chan struct{}
}

// takeSignal 获取执行信号 需要在锁内执行
func (g *Group) takeSignal(key string) *signal {
	s, ok := g.signal[key]
	if ok {
		s.taken++
		return s
	}
	s = &signal{taken: 1, ch: make(chan struct{}, 1)}
	g.signal[key] = s
	return s
}

// releaseSignal 释放执行信号 需要在锁内执行
func (g *Group) releaseSignal(key string, s *signal) {
	<-s.ch
	s.taken--
	if s.taken <= 0 {
		delete(g.signal, key)
	}
}

func (g *Group) Do(key string, resultChan chan<- interface{}, fun func() interface{}) {
	g.l.Lock()
	// fast path
	// 有同key的任务正在执行中，直接进入等待队列，等待任务执行完成
	waitList, ok := g.wait[key]
	if ok {
		waitList = append(waitList, resultChan)
		g.wait[key] = waitList
		g.l.Unlock()
		return
	}
	// low path
	// lazy init
	if g.wait == nil {
		g.wait = make(map[string][]chan<- interface{})
		g.signal = make(map[string]*signal)
	}
	// 等待队列不存在时，说明这是同key当前批次的第一个任务，没有同key的任务正在执行或者同一批的任务刚刚被取走正在执行
	waitList = make([]chan<- interface{}, 0, 4)
	waitList = append(waitList, resultChan)
	g.wait[key] = waitList

	// 获取任务执行信号
	execSignal := g.takeSignal(key)

	go func() { g.processNotify(key, execSignal, fun) }()
	g.l.Unlock()
}

func (g *Group) processNotify(key string, execSignal *signal, fun func() interface{}) {
	// 尝试获取执行信号
	// 如果阻塞，说明有同key任务正在执行，下一批第一个会阻塞在这里，后续新进来的任务会直接写入等待队列中
	// 没有阻塞，说明无同key任务正在执行，可以把当前等待队列的所有任务取出后清空，然后跑一次func方法
	execSignal.ch <- struct{}{}
	g.l.Lock()
	waitList, ok := g.wait[key]
	// 异常检查，理论上不会走到这里
	if !ok || len(waitList) == 0 {
		g.releaseSignal(key, execSignal)
		g.l.Unlock()
		return
	}
	delete(g.wait, key)
	g.l.Unlock()

	// 执行方法，执行结果通知等待中的协程
	res := fun()
	for _, waitG := range waitList {
		waitG <- res
	}

	// 释放等待信号，下一批方法开始执行
	g.l.Lock()
	g.releaseSignal(key, execSignal)
	g.l.Unlock()
}

var g Group

func Do(key string, resultChan chan<- interface{}, fun func() interface{}) {
	g.Do(key, resultChan, fun)
}
