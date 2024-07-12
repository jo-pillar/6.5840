# Lab 2: Key/Value Server

从lab2开始 还是很简单的看了一圈文档 读了以下关于线性一致性的文档就可以开始了
甚至似乎不看也没关系
线性一致性的概念有些抽象：按我的理解就是两条线上的操作 每个操作有其预期的结果 每个操作都会持续一段时间，
如果我们能把每个持续一段时间的操作给它安排一个时间点，使之能在满足其预期结果的条件下合理的安排在一条线上就是线性一致性

##
然后就是开始愉快coding了

通读了一下实验文档，把无故障和有故障的情况都看了看想一想，没高兴分开写 一把梭了

首先是client client侧不需要考虑并发，主要是考虑故障发生时的情况，
不管这个故障是发生在send Args 还是 receive Reply,客户端的感受都是我发了指令 过了一段时间没收到回复 
所以客户端的处理也比较简单，就是重发即可，那么如何辨别这是一个重发指令还是新指令呢，类似tcp 用一个自增的任务号
标识即可。

``` go

type GetArgs struct {
	Key      string
	TaskId   int
	ClientId int
	ArgId    uint
	// You'll have to add definitions here.
}
type PutAppendArgs struct {
	Key      string
	Value    string
	ClientId int
	ArgId    uint
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	ck.argId++
	args := PutAppendArgs{key, value, ck.clientId, ck.argId}
	reply := PutAppendReply{}
	ok := false
	for {
		if ok {
			break
		}
		if op == "Put" {
			ok = ck.server.Call("KVServer.Put", &args, &reply)
		} else if op == "Append" {
			ok = ck.server.Call("KVServer.Append", &args, &reply)
		}
	}//until success
	// You will have to modify this function.
	return reply.Value
}
```
接下来是服务器了
既然是kv服务器,那么首先定义一个kvcache的map 用来保存键值
然后为了避免重复操作，一个put/append 多次执行
我需要保存每个客户端上回请求的任务号以及对应的返回值，
于是服务器结构体又多了一个成员LastTask
又是map类型(emmm map真好用)他的键是客户端ID 存放的值是该客户端上回请求的任务号以及对应返回值
```go
type LastMessage struct {
	argid uint
	value string
}
type KVServer struct {
	mu       sync.Mutex
	LastTask map[int]LastMessage
	kvcache  map[string]string
	// Your definitions here.
}
```
好了基本数据结构写完了 开始写函数了
思路也很简单，就是来一个请求 我先检查是不是重复请求是，我就返回上一次的返回值，不是我就执行这次操作，并把这次操作的序列号和对应的返回值记下来，新操作会自动覆盖旧操作
```go
func  (kv *KVServer) checkDuplicate(argid uint,clientid int)(string, bool){
	kv.mu.Lock()
	defer kv.mu.Unlock()
	Message,ok:=kv.LastTask[clientid]
	if !ok{//第一次
		return "",false
	}else{
		if Message.argid==argid{
			return Message.value,true
		}
	}
	return "",false
}
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
message,ok:=kv.checkDuplicate(args.ArgId,args.ClientId)
if ok{
	reply.Value=message
}else{
	kv.mu.Lock()
	reply.Value=kv.kvcache[args.Key]
	kv.LastTask[args.ClientId]=LastMessage{args.ArgId,reply.Value}
	kv.mu.Unlock()
}
	// Your code here.
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	message,ok:=kv.checkDuplicate(args.ArgId,args.ClientId)
	if ok{
		reply.Value=message
	}else{
		kv.mu.Lock()
		//reply.Value=kv.kvcache[args.Key]
		kv.kvcache[args.Key]=args.Value
		kv.LastTask[args.ClientId]=LastMessage{args.ArgId,reply.Value}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	message,ok:=kv.checkDuplicate(args.ArgId,args.ClientId)
	if ok{
		reply.Value=message
	}else{
		kv.mu.Lock()
		reply.Value=kv.kvcache[args.Key]
		kv.kvcache[args.Key]+=args.Value
		kv.LastTask[args.ClientId]=LastMessage{args.ArgId,reply.Value}
		kv.mu.Unlock()
	}
}
```
然后 果不其然的fail了
## 调试
主要报错如下
```
Test: memory use many put clients ...
  ... Passed -- t  8.4 nrpc 100000 ops    0
Test: memory use many get client ...
--- FAIL: TestMemGetManyClients (8.07s)
    test_test.go:559: error: server using too much memory m0 34142664 m1 39352768 (52.10 per client)
Test: memory use many appends ...
2024/07/12 16:07:12 m0 490464 m1 2497136
  ... Passed -- t  1.6 nrpc  1000 ops    0
FAIL
exit status 1
FAIL    6.5840/kvsrv    36.570s
```
我用太多内存了？？  
好吧，让我来看看怎么优化。  
第一个是在一开始写的时候就不确定的问题是容错。  
假设一开始服务器中A=1，客户端a发送任务get A，B发送put A 2，  
get A在一开始发送 返回1;然后B发送put A 2，但是a没有收到get A的回复，于是a重发，那这个时候他是返回 2呢 还是返回 1呢？ It's a question.  
此外还有一个不是问题的问题，就是你说一个`put`函数，要不要返回一个旧值呢，按照定义没说，返回旧值没出错，试了试不返回也能通过测试，但是按照我之前用C++原子函数的经验，C++那边修改一个原子变量的值之后是要返回旧值的,所以一开始给每一个`put`函数都返回了旧值。  
但现在要优化了 我也只能开始试图弄明白上面这两个问题了，
懒得写了直接说结果 对于问题1，返回1和返回2都是可以的。  
对于问题2，返不返回旧值都不影响。
既然`put`和`Get`都不需要你返回旧值，那么你就无需保存，把他们的lastmessage的value 都置空，甚至`Get`连任务号都不需要保存。
```go
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	reply.Value = kv.kvcache[args.Key]
	kv.mu.Unlock()
	// Your code here.
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	_, ok := kv.checkDuplicate(args.ArgId, args.ClientId)
	if !ok {
		kv.mu.Lock()
		//reply.Value=kv.kvcache[args.Key]
		kv.kvcache[args.Key] = args.Value
		kv.LastTask[args.ClientId] = LastMessage{args.ArgId, ""}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	message, ok := kv.checkDuplicate(args.ArgId, args.ClientId)
	if ok {
		reply.Value = message
	} else {
		kv.mu.Lock()
		reply.Value = kv.kvcache[args.Key]
		kv.kvcache[args.Key] += args.Value
		kv.LastTask[args.ClientId] = LastMessage{args.ArgId, reply.Value}
		kv.mu.Unlock()
	}
}
```
