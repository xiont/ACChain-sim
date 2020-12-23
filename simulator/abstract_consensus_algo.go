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

type IAbstractConsensusAlgo interface {
	GenesisBlock() IBlock
	GenesisDagBlock() IBlock
	Mining() IAbstractMintingTask
	DagMining() IAbstractMintingTask
	IsReceivedBlockValid(receivedBlock IBlock, currentBlock IBlock) bool
	GetSelfNode() *Node
}

var _ IAbstractConsensusAlgo = new(AbstractConsensusAlgo)

type AbstractConsensusAlgo struct {
	selfNode *Node
}

func NewAbstractConsensusAlgo(selfNode *Node) *AbstractConsensusAlgo {
	return &AbstractConsensusAlgo{
		selfNode: selfNode,
	}
}

/**
 * Gets the node using this consensus algorithm.
 *
 * @return the self node
 */
func (aca *AbstractConsensusAlgo) GetSelfNode() *Node {
	return aca.selfNode
}

/**
 * Gets the genesis block.
 *
 * @return the genesis block
 */
func (aca *AbstractConsensusAlgo) GenesisBlock() IBlock {
	panic("implement me")
}

func (aca *AbstractConsensusAlgo) GenesisDagBlock() IBlock {
	panic("implement me")
}

/**
 * Minting abstract mining task.
 *
 * @return the abstract mining task
 */
func (aca *AbstractConsensusAlgo) Mining() IAbstractMintingTask {
	panic("implement me")
}

/**
 * Minting abstract mining task.
 *
 * @return the abstract mining task
 */
func (aca *AbstractConsensusAlgo) DagMining() IAbstractMintingTask {
	panic("implement me")
}
/**
 * Tests if the receivedBlock is valid with regards to the current block.
 *
 * @param receivedBlock the received block
 * @param currentBlock  the current block
 * @return true if block is valid false otherwise
 */
func (aca *AbstractConsensusAlgo) IsReceivedBlockValid(receivedBlock IBlock, currentBlock IBlock) bool {
	panic("implement me")
}
