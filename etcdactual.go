package main

import (
	"fmt"
	"os"
	"runtime"
	"time"

	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/namespace"
	"go.etcd.io/etcd/version"

	recipe "go.etcd.io/etcd/contrib/recipes"
	/*"github.com/coreos/etcd/version"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/namespace"

	recipe "github.com/coreos/etcd/contrib/recipes" */)

type Etcd struct {
	*clientv3.Client
}

func Connect(endpoints []string, prefix string) (*Etcd, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	cli.KV = namespace.NewKV(cli.KV, prefix)
	cli.Watcher = namespace.NewWatcher(cli.Watcher, prefix)
	cli.Lease = namespace.NewLease(cli.Lease, prefix)
	return &Etcd{cli}, err
}

type Queue struct {
	*recipe.Queue
}

func (etcd *Etcd) NewQueue(topic string) *Queue {
	return &Queue{recipe.NewQueue(etcd.Client, topic)}
}

var ch chan int

const QueueNum = 100

type AlertReceiver struct {
	alertQueue      []*Queue
	//runningAlertIds chan string
}

func NewAlertReceiver() *AlertReceiver {
	//cfg := config.GetInstance()

	//endpoints := strings.Split(cfg.Etcd.Endpoints, ",")
	endpoints := []string{"etcd:2379"}
	e, _ := Connect(endpoints, "alert/")
	/*if err != nil {
		panic(err)
	}*/

	alertQueue := make([]*Queue, 0)

	for i := 0; i < QueueNum; i++ {
		alertQueue = append(alertQueue, e.NewQueue(fmt.Sprintf("%s-%d", "al-job", i)))
	}

	return &AlertReceiver{
		alertQueue:      alertQueue,
		//runningAlertIds: make(chan string, 1000),
	}
}

func (ar *AlertReceiver) Serve() {
	for i := 0; i < len(ar.alertQueue); i++ {
		go ar.ExtractAlerts(i)
	}

}

func (ar *AlertReceiver) ExtractAlerts(index int) error {
	i := 0
	for {
		i++
		s, err := ar.alertQueue[index].Dequeue()
		if err != nil {
			fmt.Printf("[%s]Error: Failed to dequeque from quequ [%d]: %+v\n", time.Now().Local(), index, err)
		}
		if i > 99 {
			fmt.Printf("[%s]Dequeque 100 data from quequ [%d], latest: %s\n", time.Now().Local(), index, s)
			i=0
		}
		ch <- index
	}
}

type Executor struct {
	name          string
	alertReceiver *AlertReceiver
}

func (e *Executor) Serve() {
	e.alertReceiver.Serve()
	count := 0
	num := 0
	for {
		<-ch
		count++
		if count > 9999 {
			fmt.Printf("[%s]Dequeque total data num %d >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> RuntimeNum: %d \n", time.Now().Local(), num*10000+count, runtime.NumGoroutine())
			num ++
			count = 0
		}
	}
}

func NewExecutor(name string, alertReceiver *AlertReceiver) *Executor {
	e := &Executor{
		name:          name,
		alertReceiver: alertReceiver,
	}
	return e
}

func Init(name string) *Executor {
	alertReceiver := NewAlertReceiver()
	executor := NewExecutor(name, alertReceiver) //, aliveReporter, broadcastReceiver, healthChecker)
	return executor
}

var e *Executor

func mainFuncExecutor() {
	e = Init("host")
	e.Serve()
}

///////////////////////////////////////////////////////////////////////////////
func (ar *AlertReceiver) Enquequing(num int) {
	i := 0
	var s string
	for {
		s = fmt.Sprintf("test-%d", i)
		err := ar.alertQueue[num].Enqueue(s)
		if err != nil {
			fmt.Printf("[%s]Error:Enqueque %+v\n", time.Now().Local(), err)
		}
		ch <- num
		i++
		if i > 99 {
			fmt.Printf("[%s]Enqueue 100 to queue [%d], latest: %s\n", time.Now().Local(), num, s)
			i = 0
		}
	}
}

func (e *Executor) EnquequeServe() {
	for i := 0; i < len(e.alertReceiver.alertQueue); i++ {
		go e.alertReceiver.Enquequing(i)
	}

	fmt.Printf("[%s]Enqueque Queue num: %d\n", time.Now().Local(), runtime.NumGoroutine())

	count := 0
	num := 0
	for {
		<-ch
		count++
		if count > 9999 {
			fmt.Printf("[%s]Enqueque total data num %d >>>>>>>>>>>>>>>>>>>>>>>>>>> RuntimeNum: %d \n", time.Now().Local(), num*10000+count, runtime.NumGoroutine())
			num ++
			count = 0
		}
	}
}

func enquequeExecutor() {
	e = Init("host")
	e.EnquequeServe()
}

///////////////////////////////////////////////////////////////////////////////

func main() {
	ch = make(chan int, 10000)
	defer close(ch)

	if len(os.Args) > 1 && os.Args[1] == "-e" {
		fmt.Printf("Version %v, Enquequing...\n", version.Version)
		enquequeExecutor()
	} else {
		fmt.Printf("Version %v, Dequequing...\n", version.Version)
		mainFuncExecutor()
	}

}
