package imp

import (
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"
)

type Node struct {
	net     *NetWork // mock网络
	node_no uint32   // 节点序号

	// proposer
	proposal_idx    uint32              // 提交idx,和node_no一起组成proposal_no
	proposal_val    string              // 提交message
	promise_rsp_map map[uint32]*Message // 已经回复的promise
	accept_count    uint32              // 已经回复的accept计数
	is_failed       bool                // 是否终止

	// acceptor
	promise_no uint64 // 承诺的提交号
	accept_no  uint64 // 当前已接受的提交号
	accept_val string // 当前已接受的提交message

	// learner
	decide_val string // 当前已经学会的message

	timeout_base uint64 // 指数退避基数 ms
	timeout_exp  uint32 // 当前超时指数
}

func NewNode(node_no uint32, net *NetWork) *Node {
	return &Node{
		net:     net,
		node_no: node_no,
		// proposer
		proposal_idx:    1,
		proposal_val:    "",
		promise_rsp_map: make(map[uint32]*Message),
		accept_count:    0,
		is_failed:       false,
		// acceptor
		promise_no: 0,
		accept_no:  0,
		accept_val: "",
		// learner
		decide_val: "",
		// 超时 timeout_base * (2 ^ timeout_exp)
		timeout_base: 100,
		timeout_exp:  1,
	}
}

func (node *Node) GetNodeNo() uint32 {
	return node.node_no
}

func (node *Node) GetDecideVal() string {
	return node.decide_val
}

func (node *Node) randomDrift() uint64 {
	return rand.New(rand.NewSource(time.Hour.Microseconds())).Uint64() % 50
}

func (node *Node) newTimeout() uint64 {
	// 随机 0 ~ 50ms 的扰动
	timeout := node.timeout_base*uint64(math.Exp2(float64(node.timeout_exp))) + node.randomDrift()
	node.timeout_exp++
	return timeout
}

func (node *Node) LoopCoro(wait *sync.WaitGroup, cli_val string) {
	// 协议处理函数map
	func_map := map[MessageType]func(*Message) int32{
		// proposer
		MESSAGE_TYPE_PROMISE: node.handlePromise,
		MESSAGE_TYPE_ACCEPT:  node.handleAccept,
		MESSAGE_TYPE_FAILED:  node.handleFail,
		// acceptor
		MESSAGE_TYPE_PREPARE:  node.handlePrepare,
		MESSAGE_TYPE_PROPOSAL: node.handleProposal,
		// learner
		MESSAGE_TYPE_DECIDE: node.handleDecide,
	}
	// 随机开始延迟，减少竞争
	start_delay_timer := time.NewTimer(time.Duration(node.randomDrift()) * time.Millisecond)
	<-start_delay_timer.C
	// 超时时间
	timer := time.NewTimer(time.Duration(node.newTimeout()) * time.Millisecond)
	defer func() {
		timer.Stop()
		wait.Done()
	}()
	recv_ch := node.net.GetNodeRecvCh(node.node_no)
	if recv_ch == nil {
		fmt.Printf("Error: get recv ch for %d failed", node.node_no)
		return
	}
	// 发送初始值
	node.handleClient(&Message{Proposal_val: cli_val})
	for {
		select {
		case <-timer.C:
			if node.is_failed {
				fmt.Printf("Debug: node %d failed and restart with val: %s\n", node.node_no, cli_val)
				node.handleClient(&Message{Proposal_val: cli_val})
			}
			timer.Reset(time.Duration(node.newTimeout()) * time.Millisecond)
		case msg := <-recv_ch:
			if msg == nil {
				fmt.Println("Error: recv message nil")
				return
			}
			// fail之后只能接受acceptor或learner的协议
			if node.is_failed && msg.Msg_type != MESSAGE_TYPE_PREPARE && msg.Msg_type != MESSAGE_TYPE_PROPOSAL && msg.Msg_type != MESSAGE_TYPE_DECIDE {
				break
			}
			function := func_map[msg.Msg_type]
			if function == nil {
				fmt.Println("Error: can't find handle func for ", msg.Msg_type)
				return
			}
			ret := function(msg)
			if ret != 0 {
				return
			}
			if len(node.decide_val) > 0 {
				return
			}
		}
	}
}

func (node *Node) proposal_no() uint64 {
	return uint64(node.node_no)<<32 | uint64(node.proposal_idx)
}

/************
 * proposer
 ************/
// 处理client提交
func (node *Node) handleClient(recv_msg *Message) int32 {
	// 递增序号
	node.proposal_idx++
	node.proposal_val = recv_msg.Proposal_val
	node.promise_rsp_map = make(map[uint32]*Message)
	node.accept_count = 0
	node.is_failed = false
	node.net.Broadcast(0, &Message{
		Msg_type:     MESSAGE_TYPE_PREPARE,
		Node_no:      node.node_no,
		Proposal_no:  node.proposal_no(),
		Proposal_val: node.proposal_val,
	})
	fmt.Printf("Debug: node %d send prepare with %d\n", node.node_no, node.proposal_no())
	return 0
}

// 处理acceoptor的promise回复
func (node *Node) handlePromise(recv_msg *Message) int32 {
	if recv_msg.Proposal_no != node.proposal_no() {
		return 0
	}
	node.promise_rsp_map[recv_msg.Node_no] = recv_msg
	// 超过半数
	if node.net.IsMajority(uint32(len(node.promise_rsp_map))) {
		// 最大提交号对应的提交值
		var max_accept_no uint64 = 0
		for _, msg := range node.promise_rsp_map {
			if msg.Accept_no > max_accept_no {
				max_accept_no = msg.Accept_no
				node.proposal_val = msg.Accept_val
			}
		}
		fmt.Printf("Debug: node %d recv majority promise: %v\n", node.node_no, node.promise_rsp_map)
		node.net.Broadcast(0, &Message{
			Msg_type:     MESSAGE_TYPE_PROPOSAL,
			Node_no:      node.node_no,
			Proposal_no:  node.proposal_no(),
			Proposal_val: node.proposal_val,
		})
	}
	return 0
}

// 处理acceptor的accept回复
func (node *Node) handleAccept(recv_msg *Message) int32 {
	if recv_msg.Proposal_no != node.proposal_no() {
		return 0
	}
	node.accept_count++
	if node.net.IsMajority(node.accept_count) {
		node.decide_val = node.proposal_val
		fmt.Printf("Debug: node %d recv majority accepted and decide val: %s\n", node.node_no, node.decide_val)
		// accept成功, 发送decide
		node.net.Broadcast(0, &Message{
			Msg_type:     MESSAGE_TYPE_DECIDE,
			Node_no:      node.node_no,
			Proposal_no:  node.proposal_no(),
			Proposal_val: node.proposal_val,
		})
	}
	return 0
}

// 处理fail
func (node *Node) handleFail(recv_msg *Message) int32 {
	if recv_msg.Proposal_no != node.proposal_no() {
		return 0
	}
	node.is_failed = true
	fmt.Printf("Debug: node %d recv failed and stopped\n", node.node_no)
	return 0
}

/************
 * acceptor
 ************/
// 处理prepare协议
func (node *Node) handlePrepare(recv_msg *Message) int32 {
	// 节点已经promise
	if node.promise_no > recv_msg.Proposal_no {
		fmt.Printf("Debug: node %d recv prepare (%d < %d)\n", node.node_no, recv_msg.Proposal_no, node.promise_no)
		// 回复prepare失败
		node.net.NodeSend(recv_msg.Node_no, &Message{
			Msg_type:    MESSAGE_TYPE_FAILED,
			Node_no:     node.node_no,
			Proposal_no: recv_msg.Proposal_no,
		})
	} else {
		// 节点承诺promise
		fmt.Printf("Debug: node %d recv prepare (%d >= %d)\n", node.node_no, recv_msg.Proposal_no, node.promise_no)
		node.promise_no = recv_msg.Proposal_no
		node.net.NodeSend(recv_msg.Node_no, &Message{
			Msg_type:    MESSAGE_TYPE_PROMISE,
			Node_no:     node.node_no,
			Proposal_no: recv_msg.Proposal_no,
			Accept_no:   node.accept_no,
			Accept_val:  node.accept_val,
		})
	}
	return 0
}

// 处理proposal协议
func (node *Node) handleProposal(recv_msg *Message) int32 {
	if node.promise_no <= recv_msg.Proposal_no {
		fmt.Printf("Debug: node %d recv proposal (%d >= %d)\n", node.node_no, recv_msg.Proposal_no, node.promise_no)
		node.promise_no = recv_msg.Proposal_no
		node.accept_no = recv_msg.Proposal_no
		node.accept_val = recv_msg.Proposal_val
		node.net.NodeSend(recv_msg.Node_no, &Message{
			Msg_type:    MESSAGE_TYPE_ACCEPT,
			Node_no:     node.node_no,
			Proposal_no: recv_msg.Proposal_no,
		})
	} else {
		fmt.Printf("Debug: node %d recv proposal (%d < %d)\n", node.node_no, recv_msg.Proposal_no, node.promise_no)
		// 回复proposer失败
		node.net.NodeSend(recv_msg.Node_no, &Message{
			Msg_type:    MESSAGE_TYPE_FAILED,
			Node_no:     node.node_no,
			Proposal_no: recv_msg.Proposal_no,
		})
	}
	return 0
}

/************
 * learner
 ************/
// 处理decide协议
func (node *Node) handleDecide(recv_msg *Message) int32 {
	if len(node.decide_val) > 0 {
		fmt.Println("Error: already has decide val: ", node.decide_val)
	}
	node.decide_val = recv_msg.Proposal_val
	fmt.Printf("Debug: node %d recv learn val: %s\n", node.node_no, node.decide_val)
	return 0
}
