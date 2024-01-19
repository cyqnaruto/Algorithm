package imp

import "fmt"

type MessageType int

const (
	MESSAGE_TYPE_PREPARE  MessageType = 1
	MESSAGE_TYPE_PROMISE  MessageType = 2
	MESSAGE_TYPE_PROPOSAL MessageType = 3
	MESSAGE_TYPE_ACCEPT   MessageType = 4
	MESSAGE_TYPE_FAILED   MessageType = 5
	MESSAGE_TYPE_CLIENT   MessageType = 6
	MESSAGE_TYPE_DECIDE   MessageType = 7
)

type Message struct {
	Msg_type     MessageType // message类别
	Node_no      uint32      // 节点序号，用于通信
	Proposal_no  uint64      // 提案号
	Proposal_val string      // 提案内容
	Accept_no    uint64      // 已经接受的提案号
	Accept_val   string      // 已经接受的提案值
}

type NetWork struct {
	nodes_net_map map[uint32]chan *Message
}

func NewNetWork(node_no_vec []uint32) *NetWork {
	network := &NetWork{
		nodes_net_map: make(map[uint32]chan *Message),
	}
	for _, node_no := range node_no_vec {
		network.nodes_net_map[node_no] = make(chan *Message, 100)
	}
	return network
}

// 是否超过半数
func (net *NetWork) IsMajority(cnt uint32) bool {
	return cnt == uint32(len(net.nodes_net_map)+1)/2
}

// 收包
func (net *NetWork) GetNodeRecvCh(node_no uint32) chan *Message {
	if ch, ok := net.nodes_net_map[node_no]; ok {
		return ch
	} else {
		fmt.Printf("Error: can't find net for node %d\n", node_no)
		return nil
	}
}

// 发包
func (net *NetWork) NodeSend(node_no uint32, msg *Message) {
	if ch, ok := net.nodes_net_map[node_no]; ok {
		ch <- msg
	} else {
		fmt.Printf("Error: can't find net for node %d\n", node_no)
	}
}

// 广播
func (net *NetWork) Broadcast(except_node_no uint32, msg *Message) {
	for node_no, node_net := range net.nodes_net_map {
		if node_no == except_node_no {
			continue
		}
		node_net <- msg
	}
}
