/*
   Copyright 2020 LittleBear(1018589158@qq.com)

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package simulator

import (
	"acchain-sim/graph"
	"acchain-sim/printfile"
	"acchain-sim/settings"
	"acchain-sim/utils"
	"container/list"
	"fmt"
	"github.com/emirpasic/gods/lists"
	"github.com/emirpasic/gods/lists/arraylist"
	"github.com/emirpasic/gods/sets/hashset"
	"strconv"
)

type INode interface {
	getNodeId() int
	getRegion() int
	getMiningPower() int64
}

/**
 * A class representing a node in the network.
 */
type Node struct {
	/**
	 * Unique node ID.
	 */
	nodeID int

	/**
	 * Region assigned to the node.
	 */
	region int

	/**
	 * Mining power assigned to the node.
	 */
	miningPower int64

	/**
	 * A nodes routing table.
	 */
	routingTable IAbstractRoutingTable

	/**
	 * The consensus algorithm used by the node.
	 */
	consensusAlgo IAbstractConsensusAlgo

	/**
	 * The current block.
	 */
	block IBlock

	/**
	 * Orphaned blocks known to node.
	 */
	// TODO 实例化
	orphans *hashset.Set

	/**
	 * The current mining task(default for chain-block mining)
	 */
	miningTask ITask

	/**
	 * The current dag-block mining task
	 */
	miningDagTask ITask

	/**
	 * In the process of sending blocks.
	 */
	sendingBlock bool
	lastSendTime int64


	toSendBlocks *list.List

	downloadedBlocks lists.List


	processingTime int

	currentGraph *graph.Graph
}

/**
 * Instantiates a new Node.
 *
 * @param nodeID            the node id
 * @param numConnection     the number of connections a node can have
 * @param region            the region
 * @param miningPower       the mining power
 * @param routingTableName  the routing table name
 * @param consensusAlgoName the consensus algorithm name
 */
func NewNode(nodeID int, numConnection int,
	region int, miningPower int64,
) *Node {

	node := &Node{
		nodeID:      nodeID,
		region:      region,
		miningPower: miningPower,
		//routingTable:      routingTable,
		//consensusAlgo:     consensusAlgo,
		block:            nil,
		orphans:          hashset.New(),
		miningTask:       nil,
		miningDagTask: 	  nil,
		sendingBlock:     false,
		toSendBlocks:     new(list.List),
		downloadedBlocks: arraylist.New() ,
		processingTime:   settings.PROCESS_TIME,
		lastSendTime: 0,
		currentGraph: graph.New(),
	}

	// Using the reflect function to find the Initial TABLE and ALGO
	r1, _ := utils.Call(*settings.FUNCS, settings.TABLE, node)
	node.SetRoutingTable(r1[0].Interface().(IAbstractRoutingTable))
	r2, _ := utils.Call(*settings.FUNCS, settings.ALGO, node)
	node.SetConsensusAlgo(r2[0].Interface().(IAbstractConsensusAlgo))
	node.setNumConnection(numConnection)

	return node
}

/**
 * Gets the node id.
 *
 * @return the node id
 */
func (n *Node) GetNodeID() int {
	return n.nodeID
}

/**
 * Gets the region ID assigned to a node.
 *
 * @return the region
 */
func (n *Node) GetRegion() int {
	return n.region
}

/**
 * Gets mining power.
 *
 * @return the mining power
 */
func (n *Node) GetMiningPower() int64 {
	return n.miningPower
} /**


 * Gets the consensus algorithm.
 *
 * @return the consensus algorithm. See {@link AbstractConsensusAlgo}
 */
func (n *Node) SetConsensusAlgo(algo IAbstractConsensusAlgo) {
	n.consensusAlgo = algo
}

/**
 * Gets routing table.
 *
 * @return the routing table
 */
func (n *Node) SetRoutingTable(table IAbstractRoutingTable) {
	n.routingTable = table
}

func (n *Node) getConsensusAlgo() IAbstractConsensusAlgo {
	return n.consensusAlgo
}

/**
 * Gets routing table.
 *
 * @return the routing table
 */
func (n *Node) getRoutingTable() IAbstractRoutingTable {
	return n.routingTable
}

/**
 * Gets the current block.
 *
 * @return the block
 */
func (n *Node) GetBlock() IBlock {
	return n.block
}

/**
 * Gets all orphans known to node.
 *
 * @return the orphans
 */
func (n *Node) GetOrphans() *hashset.Set {
	return n.orphans
}

/**
 * Gets the number of connections a node can have.
 *
 * @return the number of connection
 */
func (n *Node) GetNumConnection() int {
	return n.routingTable.GetNumConnection()
}

/**
 * Sets the number of connections a node can have.
 *
 * @param numConnection the n connection
 */
func (n *Node) setNumConnection(numConnection int) {
	n.routingTable.SetNumConnection(numConnection)
}

/**
 * Gets the nodes neighbors.
 *
 * @return the neighbors
 */
func (n *Node) GetNeighbors() *arraylist.List {
	return n.routingTable.GetNeighbors()
}

/**
 * Adds the node as a neighbor.
 *
 * @param node the node to be added as a neighbor
 * @return the success state of the operation
 */
func (n *Node) addNeighbor(node *Node) bool {
	return n.routingTable.AddNeighbor(node)
}

/**
 * Removes the neighbor form the node.
 *
 * @param node the node to be removed as a neighbor
 * @return the success state of the operation
 */
func (n *Node) removeNeighbor(node *Node) bool {
	return n.routingTable.RemoveNeighbor(node)
}

/**
 * Initializes the routing table.
 */
func (n *Node) JoinNetWork() {
	n.routingTable.InitTable()
}

/**
 * Mint the genesis block.
 */
func (n *Node) GenesisBlock() {
	genesis := n.consensusAlgo.GenesisBlock()
	DagGenesis := n.consensusAlgo.GenesisDagBlock()
	n.receiveBlock(genesis)
	n.receiveBlock(DagGenesis)
}

/**
 * Adds a new block to the to chain. If node was mining that task instance is abandoned, and
 * the new block arrival is handled.
 *
 * @param newBlock the new block
 */
func (n *Node) addToChain(newBlock IBlock) {
	// If the node has been mining
	if n.miningTask != nil {
		//Timer.removeTask
		RemoveTask(n.miningTask)
		n.miningTask = nil
	}
	// Update the current block
	n.block = newBlock
	n.printAddBlock(newBlock)
	// Observe and handle new block arrival
	// Simulator.arriveBlock 通知模拟器到达了区块
	Simulator.ArriveBlock(newBlock, n)
}

/**
 * Logs the provided block to the logfile.
 *
 * @param newBlock the block to be logged
 */
// TODO
func (n *Node) printAddBlock(newBlock IBlock) {
	printfile.OUT_JSON_FILE.Print("{")
	printfile.OUT_JSON_FILE.Print("\"kind\":\"add-block\",")
	printfile.OUT_JSON_FILE.Print("\"content\":{")
	printfile.OUT_JSON_FILE.Print("\"timestamp\":" + strconv.FormatInt(GetCurrentTime(), 10) + ",")
	printfile.OUT_JSON_FILE.Print("\"node-id\":" + strconv.Itoa(n.GetNodeID()) + ",")
	printfile.OUT_JSON_FILE.Print("\"block-id\":" + strconv.Itoa(newBlock.GetId()))
	printfile.OUT_JSON_FILE.Print("}")
	printfile.OUT_JSON_FILE.Print("},")
	printfile.OUT_JSON_FILE.Flush()
	//OUT_JSON_FILE.flush()
}

/**
 * Add orphans.
 *
 * @param orphanBlock the orphan block
 * @param validBlock  the valid block
 */
//TODO check this out later
func (n *Node) addOrphans(orphanBlock IBlock, validBlock IBlock) {
	if orphanBlock != validBlock {
		n.orphans.Add(orphanBlock)
		n.orphans.Remove(validBlock)
		if validBlock == nil || orphanBlock.GetHeight() > validBlock.GetHeight() {
			n.addOrphans(orphanBlock.GetParent(), validBlock)
		} else if orphanBlock.GetHeight() == validBlock.GetHeight() {
			n.addOrphans(orphanBlock.GetParent(), validBlock.GetParent())
		} else {
			n.addOrphans(orphanBlock, validBlock.GetParent())
		}
	}
}

/**
 * Generates a new mining task and registers it
 */
func (n *Node) mining() {
	miningTask := n.consensusAlgo.Mining()
	n.miningTask = miningTask
	if miningTask != nil {
		PutTask(miningTask)
	}
}

/**
 * Generates a new mining task and registers it
 */
func (n *Node) miningDag() {
	miningDagTask := n.consensusAlgo.DagMining()
	n.miningDagTask = miningDagTask
	if miningDagTask != nil {
		PutTask(miningDagTask)
	}
}

/**
 * Receive block.
 *
 * @param block the block
 */
// TODO
func (n *Node) receiveBlock(block IBlock) {

	if block.GetType() == settings.CHAIN_BLOCK{
		if n.consensusAlgo.IsReceivedBlockValid(block, n.block) {
			//如果共识协议判断该区块是正确的
			if n.block != nil && !n.block.IsOnSameChainAs(block) {
				// 如果主链的最新区块不是空的 ，并且给的区块不在本条主链上， 那么加入到孤块中
				// If orphan mark orphan
				n.addOrphans(n.block, block)
			}
			// Else add to canonical chain
			// 添加到达区块的同时，废弃本节点的挖矿任务，同时通知模拟器区块的到达
			n.addToChain(block)
			// Generates a new mining task
			// 新建挖矿任务
			n.mining()

			n.currentGraph = graph.New()

			// TODO update DAGMing
			if n.miningDagTask !=nil{
				RemoveTask(n.miningDagTask)
			}
			n.miningDag()
			n.miningTask.(*MintingTask).SetConfirmedBlocks([]IBlock{})
			n.miningDagTask.(*MintingTask).SetConfirmedBlocks([]IBlock{})

			// Advertise received block
			// 广播给其他节点区块的到达
			// 此处可以优化一点点（不要再发给我了）
			n.toSendBlocks.PushBack(block)
			if !n.sendingBlock {
				n.SendNextBlockMessage()
			}
		} else if !n.orphans.Contains(block) && !block.IsOnSameChainAs(n.block) {
			// 如果共识协议不正确，如果本节点的孤块列表中没有包含该区块 ， 并且该区块也不在主链上
			// 加入到孤块中，直接通知模拟器区块的到达
			// TODO better understand - what if orphan is not valid?
			// If the block was not valid but was an unknown orphan and is not on the same chain as the
			// current block
			// 会根据孤块的父区块 和 本区块一直追溯，找到交叉点
			n.addOrphans(block, n.block)
			Simulator.ArriveBlock(block, n)
		}
	}else if block.GetType() == settings.DAG_BLOCK{
		//fmt.Printf("%v,%v\n",n.GetNodeID() ,block.GetType())



		// TODO handle and restart DAG_Mining
		if n.miningDagTask !=nil{
			RemoveTask(n.miningDagTask)
		}
		n.miningDag()

		// TODo handle graph
		if block.GetParent() == n.block{
			n.currentGraph.AddNode(block)
			for _,node := range block.GetConfirmedBlocks(){
				fmt.Print("here")
				n.currentGraph.AddEdge(block,node)
			}
			var tmpConfirmedBlocks []IBlock
			for _,node := range n.currentGraph.GetNoIndegreeNodes(){
				tmpConfirmedBlocks = append(tmpConfirmedBlocks, node.(IBlock))
			}
			n.miningTask.(*MintingTask).SetConfirmedBlocks(tmpConfirmedBlocks)
			n.miningDagTask.(*MintingTask).SetConfirmedBlocks(tmpConfirmedBlocks)
		}


		n.toSendBlocks.PushBack(block)
		if !n.sendingBlock {
			n.SendNextBlockMessage()
		}
	}


}

/**
 * Receive message.
 *
 *      BlockMessage -->
 * @param message the message
 */
func (n *Node) ReceiveMessage(message IAbstractMessageTask) {

	if blockMessage, ok := message.(*BlockMessageTask); ok {

		block := blockMessage.GetBlock()
		if n.downloadedBlocks.Contains(block){
			// 如果接收到的区块最近已经下载过了，那么就丢弃
			return
		}
		if n.downloadedBlocks.Size() > settings.THROUGHPUT*2 {
			// 如果接收到的区块个数大于一定了阈值，那么删除比较早接收到的区块
			n.downloadedBlocks.Remove(0)
		}

		// 接收到的区块为新的区块，那么对其进行处理
		// 节点接收到区块后会再次开始挖矿和广播
		n.downloadedBlocks.Add(block)
		n.receiveBlock(block)

	}
}

/**
 * Send next block message.
 */
// send a block to the sender of the next queued recMessage
// send 之后，将队列中的消息移除
func (n *Node) SendNextBlockMessage() {

	if n.toSendBlocks.Len() > 0 {
		n.sendingBlock = true




		block := n.toSendBlocks.Front().Value
		it := n.routingTable.GetNeighbors().Iterator()
		for j:=1;it.Next();j++ {

			value :=  it.Value()

			bandwidth := GetBandwidth(n.GetRegion(), value.(*Node).GetRegion())

			// Convert bytes to bits and divide by the bandwidth expressed as bit per millisecond, add
			// processing time.
			delay := (settings.BLOCK_SIZE*8/(bandwidth/1000) + int64(n.processingTime))*int64(j)

			blockMessageTask := NewBlockMessageTask(n, value.(*Node), block.(IBlock), delay)
			//println(n.GetNodeID(),value.(*Node).GetNodeID() ,blockMessageTask.GetInterval())
			PutTask(blockMessageTask)
		}
		n.toSendBlocks.Remove(n.toSendBlocks.Front())


	} else {
		n.sendingBlock = false
	}
}
