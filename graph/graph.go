package graph

import (
	"errors"
	"fmt"
	"github.com/emirpasic/gods/sets/hashset"
	"github.com/emirpasic/gods/sets/linkedhashset"
	"github.com/emirpasic/gods/stacks/arraystack"
	"sort"
)


type Graph struct {
	nodeList   []interface{}
	lookup     map[interface{}]int
	nodeMap    map[interface{}][]interface{}
	indegreeMap map[interface{}][]interface{}
	root       interface{}
	noIndegreeNodes *hashset.Set
	usefulSet *hashset.Set
}

func New() *Graph{
	return &Graph{
		nodeList:   []interface{}{},
		lookup:     make(map[interface{}]int),
		nodeMap:    make(map[interface{}][]interface{}),
		indegreeMap: make(map[interface{}][]interface{}),
		noIndegreeNodes: hashset.New(),
		usefulSet: hashset.New(),
	}
}

func (G *Graph)HaveNode(node interface{}) (error,bool){
	_,ok := G.lookup[node]
	if !ok{
		return errors.New(fmt.Sprintf("lookup cannot find such %v",node) ),ok
	}
	return nil,ok
}

func (G *Graph)AddNode(node interface{}) error{
	_,ok := G.HaveNode(node)
	if ok{
		return errors.New(fmt.Sprintf("Graph has such a node %v",node))
	}
	G.nodeList = append(G.nodeList,node)
	G.lookup[node] = len(G.nodeList) -1
	return nil
}

func (G *Graph)AddEdge(node1 interface{},node2 interface{}) error{
	err1,ok1 := G.HaveNode(node1)
	err2,ok2 := G.HaveNode(node2)
	if !ok1 || !ok2{
		panic(errors.New(err1.Error()+"||"+err2.Error()))
		// return  errors.New(err1.Error()+"||"+err2.Error()), false
	}
	if node1 == node2{
		//panic(errors.New(fmt.Sprintf("node %v is same with node %v",node1,node2) ))
		return  errors.New(fmt.Sprintf("node %v is same with node %v",node1,node2) )
	}
	G.nodeMap[node1] = append(G.nodeMap[node1], node2)
	G.indegreeMap[node2] = append(G.indegreeMap[node2], node1)

	G.noIndegreeNodes.Remove(node2)
	if _,ok := G.indegreeMap[node1];!ok{
		G.noIndegreeNodes.Add(node1)
	}
	return nil
}


func (G *Graph)SetRoot(node interface{}) error{
	err,ok := G.HaveNode(node)
	if !ok{
		panic(err)
		return err
	}
	G.root = node
	G.indegreeMap[node] = []interface{}{}

	G.noIndegreeNodes.Clear()
	G.noIndegreeNodes.Add(node)
	return nil
}


func (G *Graph)Child(node interface{}) []interface{}{
	err,ok := G.HaveNode(node)
	if !ok{
		panic(err)
	}
	children,_ := G.nodeMap[node]

	return children
}


var usefulIdList *linkedhashset.Set
func (G *Graph)addUsefulId(children []interface{}){
	if len(children)>0 {
		for _,node := range children{
			usefulIdList.Add(G.lookup[node])
			//usefulIdList = append(usefulIdList, G.lookup[node])
			G.addUsefulId(G.Child(node))
		}
	}
}


/*
	its a low performance method
 */
func (G *Graph)SubGraph(node interface{}){
	// also trim useless blocks and edges
	err,ok := G.HaveNode(node)
	if !ok{
		panic(err)
	}

	L := hashset.New()

	stack := arraystack.New()
	stack.Push(node)
	var tmpNode interface{}
	for !stack.Empty(){
		tmpNode, _ = stack.Pop()
		L.Add(tmpNode)
		if len(G.nodeMap[tmpNode])>0{
			for _,node := range G.nodeMap[tmpNode]{
				stack.Push(node)
			}
		}
	}


	for _,node := range G.nodeList{
		if !L.Contains(node){
			delete(G.lookup,node)
			delete(G.nodeMap,node)
		}
	}



	G.nodeList = L.Values()

	for key,node := range G.nodeList{
		G.lookup[node] = key
	}

	newIndegreeMap := make(map[interface{}][]interface{})
	for key,nodes := range G.nodeMap{
		for _,node := range nodes {
			newIndegreeMap[node] = append(newIndegreeMap[node], key)
		}
	}
	G.indegreeMap = newIndegreeMap

	//fmt.Printf("%v",G.indegreeMap)
	G.root = node

	G.noIndegreeNodes.Clear()
	G.noIndegreeNodes.Add(node)
}

func contain(v []interface{},i interface{}) int  {
	for key,value := range v{
		if value==i{
			return key
		}
	}
	return -1
}


func (G *Graph)makeUsefulItem(set []interface{},visited map[interface{}]bool){
	if len(set) > 0{
		for _,node := range set{
			G.usefulSet.Add(node)
			if _,ok := visited[node];!ok{
				G.makeUsefulItem(G.Child(node),visited)
				visited[node] = true
			}
		}
	}
}

func (G *Graph)MakeUsefulItem(){
	G.usefulSet.Clear()
	G.usefulSet.Add(G.root)
	visited := make(map[interface{}]bool)
	G.makeUsefulItem(G.Child(G.root),visited)
}

func (G *Graph)LogicSort( cmp func(interface{},interface{}) bool) []interface{}{
	G.MakeUsefulItem()

	L := linkedhashset.New() // result set
	S := linkedhashset.New() // node set that in-degree = 0
	S.Add(G.root)

	for S.Size()>0 {
		var L2 []interface{}
		node := S.Values()[0]
		L.Add(node)
		S.Remove(node)

		// add items no in-degree to S except in L
		for key,value := range G.indegreeMap{
			if !L.Contains(key) && G.usefulSet.Contains(key){
				co := contain(value,node)
				if co != -1{
					G.indegreeMap[key] = append(G.indegreeMap[key][0:co],G.indegreeMap[key][co+1:]... )

				}

				i_ := 0
				for id,node := range G.indegreeMap[key]{
					if !G.usefulSet.Contains(node){
						G.indegreeMap[key] = append(G.indegreeMap[key][0:id-i_],G.indegreeMap[key][id-i_+1:]... )
						i_++
					}
				}

				if len(G.indegreeMap[key]) == 0{
					L2 = append(L2, key)
				}
			}
		}

		sort.Slice(L2, func(i, j int) bool {
			return cmp(L2[i],L2[j])
		})

		S.Add(L2...)
	}

	return L.Values()
}


func (G *Graph)GetNoIndegreeNodes() []interface{}{
	return G.noIndegreeNodes.Values()
}

func (G *Graph)GetAllNodes() []interface{}{
	return G.nodeList
}
func (G *Graph)GetAllEdges() map[interface{}][]interface{}{
	return G.nodeMap
}

//func main() {
//	block1 := block.New("a")
//	block2 := block.New("b")
//	block3 := block.New("c")
//	block4 := block.New("d")
//	block5 := block.New("e")
//
//	block6 := block.New("f")
//	block7 := block.New("g")
//
//	block1_ := block.New("g")
//
//	graph := New()
//	err := graph.AddNode(block1)
//	if err != nil{
//		println(err.Error())
//	}
//	_ = graph.AddNode(block2)
//	_ = graph.AddNode(block3)
//	_ = graph.AddNode(block4)
//	_ = graph.AddNode(block5)
//
//	_ = graph.AddNode(block6)
//	_ = graph.AddNode(block7)
//
//	_ = graph.AddNode(block1_)
//
//
//	_ = graph.AddEdge(block1, block2)
//	_ = graph.AddEdge(block1, block3)
//	_ = graph.AddEdge(block2, block3)
//	_ = graph.AddEdge(block3, block4)
//	_ = graph.AddEdge(block4, block5)
//
//	_ = graph.AddEdge(block1,block6)
//	_ = graph.AddEdge(block6,block3)
//	_ = graph.AddEdge(block4,block7)
//
//	_ = graph.AddEdge(block6,block7)
//
//	//graph.SubGraph(block2)
//	graph.SetRoot(block3)
//
//	L:=graph.LogicSort(func(i interface{}, i2 interface{}) bool {
//		// favoring the smaller one
//		return  i.(*block.Block).GetName() < i2.(*block.Block).GetName()
//	})
//
//	fmt.Printf("usefulSet %v\n",graph.usefulSet)
//
//	fmt.Printf("noindegreenodes %v\n",graph.noIndegreeNodes)
//
//	for _,v := range L{
//		fmt.Printf("%v",v)
//	}
//
//}
