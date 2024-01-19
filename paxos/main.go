package main

import (
	"fmt"
	"math/rand"
	"paxos/imp"
	"strconv"
	"sync"
	"time"
)

func main() {
	node_no_vec := []uint32{1, 2, 3, 4, 5}
	for i := 0; i < 1000; i++ {
		network := imp.NewNetWork(node_no_vec)
		if network == nil {
			fmt.Println("Error: create network failed")
			return
		}
		node_vec := []*imp.Node{}
		for _, node_no := range node_no_vec {
			node := imp.NewNode(node_no, network)
			if node == nil {
				fmt.Println("Error: create node failed")
				return
			}
			node_vec = append(node_vec, node)
		}
		wait := &sync.WaitGroup{}
		wait.Add(len(node_vec))
		rand.New(rand.NewSource(time.Now().UnixNano())).Shuffle(len(node_vec), func(i, j int) {
			node_vec[i], node_vec[j] = node_vec[j], node_vec[i]
		})
		for _, node := range node_vec {
			go node.LoopCoro(wait, "node_"+strconv.FormatUint(uint64(node.GetNodeNo()), 10))
		}
		wait.Wait()
		var decide_val string = ""
		for _, node := range node_vec {
			if len(decide_val) <= 0 {
				decide_val = node.GetDecideVal()
				continue
			}
			if node.GetDecideVal() != decide_val {
				decide_val_map := map[uint32]string{}
				for _, node := range node_vec {
					decide_val_map[node.GetNodeNo()] = node.GetDecideVal()
				}
				fmt.Printf("Error: decide failed: %v\n", decide_val_map)
				return
			}
		}
		fmt.Printf("Info: finish round %d with decide value %s\n", i, decide_val)
	}
}
