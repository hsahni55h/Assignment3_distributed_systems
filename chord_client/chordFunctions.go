package main

import (
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const (
	RESOURCES_FOLDER = "./resources"
	FILE_PRIVILEGES            = 0o600
	DIR_PRIVILEGES             = 0o700
)

type NodeDetails struct {
	IPaddress      string
	Port    int
	SecurePort int
	ID         big.Int
}

type Node struct {
	Details            NodeDetails
	FingerTable     []NodeDetails
	FingerTableSize int
	Predecessor     *NodeDetails
	Successors      []NodeDetails
	SuccessorsSize  int
	NextFinger      int
}

var once sync.Once
var nodeInstance Node

func InitializeNode(ownIp string, ownPort, securePort, fingerTableCount, successorsCount int, additionalId *big.Int) error {
	if fingerTableCount < 1 || successorsCount < 1 {
		return errors.New("sizes need to be at least 1")
	}
	once.Do(func() {
		var address = string(ownIp) + ":" + string(fmt.Sprintf("%v", ownPort))
		var Details NodeDetails
		if additionalId == nil {
			Details = NodeDetails{
				IPaddress:      ownIp,
				Port:    ownPort,
				SecurePort: securePort,
				ID:         *GenerateHash(string(address)),
			}
		} else {
			Details = NodeDetails{
				IPaddress:      ownIp,
				Port:    ownPort,
				SecurePort: securePort,
				ID:         *additionalId,
			}
		}
		nodeInstance = Node{
			Details:            Details,
			FingerTable:     []NodeDetails{},
			Predecessor:     nil,
			FingerTableSize: fingerTableCount,
			Successors:      []NodeDetails{},
			SuccessorsSize:  successorsCount,
			NextFinger:      -1,
		}
	})
	return nil
}

func FetchChordAddress(node NodeDetails) string {
	return fmt.Sprintf("%v:%v", node.IPaddress, node.Port)
}

func FetchSshAddress(node NodeDetails) string {
	return fmt.Sprintf("%v:%v", node.IPaddress, node.SecurePort)
}

func IncrementFollowFinger() int {
	nodeInstance.NextFinger = (nodeInstance.NextFinger + 1) % nodeInstance.FingerTableSize
	return nodeInstance.NextFinger
}

func UpdateSuccessor(successor NodeDetails) {
	nodeInstance.Successors = []NodeDetails{successor}
}

func UpdateFingerTable(successor NodeDetails) {
	nodeInstance.FingerTable = []NodeDetails{successor}
}

func CheckSuccesorsContainItself(successors []NodeDetails) int {
	for index, item := range successors {
		if item.ID.Cmp(&nodeInstance.Details.ID) == 0 {
			return index
		}
	}
	return -1
}

// Append successor's successors to the node's list of successors.
func AddSuccessors(successors []NodeDetails) {
	ElementsCount := nodeInstance.SuccessorsSize - 1
	var addElements []NodeDetails
	// To avoid panic
	if len(successors) > ElementsCount {
		addElements = successors[:ElementsCount]
	} else {
		addElements = successors
	}
	index := CheckSuccesorsContainItself(addElements)
	if index != -1 {
		addElements = addElements[:index]
	}
	nodeInstance.Successors = append(nodeInstance.Successors, addElements...)
}

func Successor() NodeDetails {
	return nodeInstance.Successors[0]
}

func SetPredecessor(predecessor *NodeDetails) {
	nodeInstance.Predecessor = predecessor
}

func Get() Node {
	return nodeInstance
}

func Lookup(fileKey big.Int) (*NodeDetails, error) {
	node := Get()
	foundNode, err := FindNode(fileKey, node.Details, 32)
	if err != nil {
		return nil, err
	}
	return foundNode, nil
}

func StoreFile(fileLoc string, ssh bool, encrypt bool) (*NodeDetails, *big.Int, error) {
	Loc := strings.Split(fileLoc, "/")
	fileName := Loc[len(Loc)-1]
	fileIdentifier := GenerateHash(fileName)
	node, err := Lookup(*fileIdentifier)
	if err != nil {
		return nil, nil, err
	}
	data, err := FileRead(fileLoc)
	if err != nil {
		return nil, nil, err
	}
	if encrypt {
		cipherData, err := EncryptData(data)
		if err != nil {
			return nil, nil, err
		}
		data = cipherData
	}
	log.Printf("Attempting to store file at %v", *node)
	if ssh {
		fileName := fileIdentifier.String()
		err3 := TransmitFile(FetchSshAddress(*node), fileName, data)
		if err3 != nil {
			return nil, nil, err3
		}
	} else {
		err := SaveClientFile(FetchChordAddress(*node), *fileIdentifier, data)
		if err != nil {
			return nil, nil, err
		}
	}

	return node, fileIdentifier, nil
}

func FetchNodeState(node NodeDetails, collectionItem bool, index int, idealIdentifier *big.Int) (*string, error) {
	NodeDetails := fmt.Sprintf("Identifier: %v address: %v:%v SecurePort: %v", node.ID.String(), node.IPaddress, node.Port, node.SecurePort)
	if collectionItem {
		NodeDetails += fmt.Sprintf("\nIndex: %v\nIdeal Identifier: %v", index, idealIdentifier)
	}
	NodeDetails += "\n"
	return &NodeDetails, nil
}

type CalculateIdealIdentifier func(int) big.Int

func FetchNodeArrayState(nodes []NodeDetails, calculateIdealIdentifier CalculateIdealIdentifier) (*string, error) {
	status := new(string)
	for position, object := range nodes {
		idealIdentifier := calculateIdealIdentifier(position)
		info, err := FetchNodeState(object, true, position, &idealIdentifier)
		if err != nil {
			return nil, err
		}
		*status += *info + "\n\n"
	}
	return status, nil
}

func FetchState() (*string, error) {
	node := Get()
	status, err := FetchNodeState(node.Details, false, -1, nil)
	if err != nil {
		return nil, err
	}
	*status += "Predecessor: "
	if node.Predecessor == nil {
		*status += "None \n"
	} else {
		*status += node.Predecessor.ID.String()
	}

	*status += "\n\nSuccessors:\n"

	successorStatus, err := FetchNodeArrayState(node.Successors, func(i int) big.Int { return *new(big.Int).Add(big.NewInt(int64(i+1)), &node.Details.ID) })
	if err != nil {
		return nil, err
	}
	if successorStatus == nil {
		*status += "No successors\n"
	} else {
		*status += *successorStatus
	}

	*status += "\nFinger table:\n"

	fingerTableStatus, err := FetchNodeArrayState(node.FingerTable, func(i int) big.Int { return *Jump(node.Details.ID, i) })
	if err != nil {
		return nil, err
	}
	if fingerTableStatus == nil {
		*status += "No finger table entries\n"
	} else {
		*status += *fingerTableStatus
	}
	return status, nil
}

func SearchSuccessor(id big.Int) (bool, NodeDetails) {
	node := Get()
	if Within(&node.Details.ID, &id, &node.Successors[0].ID, true) {
		log.Printf("Successor search Id: %v, result: %v, %v", id.String(), true, node.Successors[0])
		return true, node.Successors[0]
	}
	nodeNearest := NearestPrecedingNode(id)
	log.Printf("Successor search Id: %v, result: %v, %v", id.String(), false, nodeNearest)
	return false, nodeNearest
}

func FindNode(id big.Int, start NodeDetails, Steps int) (*NodeDetails, error) {
	identified := false
	successorNode := start
	for i := 0; i < Steps; i++ {
		var res, err = RpcSearchSuccessor(FetchChordAddress(successorNode), &id)
		if err != nil {
			return nil, err
		}
		identified = res.Found
		if identified {
			return &res.Node, nil
		}
		successorNode = res.Node
	}
	return nil, errors.New("successor not found")
}

func FindNearPrecedingCandidate(n Node, table []NodeDetails, id big.Int) *NodeDetails {
	for i := len(table) - 1; i >= 0; i-- {
		if Within(&n.Details.ID, &table[i].ID, &id, false) {
			return &table[i]
		}
	}
	return nil
}

func NearestPrecedingNode(id big.Int) NodeDetails {
	node := Get()
	var candidate *NodeDetails = FindNearPrecedingCandidate(node, node.FingerTable, id)
	if c := FindNearPrecedingCandidate(node, node.Successors, id); candidate == nil || (c != nil &&
		Within(&id, &c.ID, &candidate.ID, false)) {
		candidate = c
	}
	if candidate != nil {
		log.Printf("Near preceding node id: %v, result: %v\n", id, *candidate)
		return *candidate
	}
	log.Printf("Near preceding node id: %v, result: %v\n", id, node.Details)
	return node.Details
}

func Begin(ownIp string, ownPort, securePort, fingerTableCount, successorsCount int, initNewRing bool, joinIp *string, joinPort *int, additionalId *big.Int) error {
	log.Printf("node started %v:%v with securePort port: %v", ownIp, ownPort, securePort)
	// Predecessor is sets to nil
	err := InitializeNode(ownIp, ownPort, securePort, fingerTableCount, successorsCount, additionalId)
	if err != nil {
		return err
	}
	if initNewRing {
		CreateRing()
		return nil
	}
	if joinIp == nil || joinPort == nil {
		return errors.New("if createNewRing is set to false, join IP address and join port are required")
	}
	temp_Id := Get().Details.ID
	return JoinRing(*joinIp, *joinPort, &temp_Id, fingerTableCount)
}

func CreateRing() {
	UpdateSuccessor(Get().Details)
	UpdateFingerTable(Get().Details)
}

func JoinRing(joinIp string, joinPort int, nodeId *big.Int, maxSteps int) error {
	// Dummy values only for the method to work
	joinHash := big.NewInt(-1)
	securePort := -1

	successor, err := FindNode(*nodeId, NodeDetails{IPaddress: joinIp, Port: joinPort, SecurePort: securePort, ID: *joinHash}, maxSteps)
	if err != nil {
		return err
	}
	UpdateSuccessor(*successor)
	UpdateFingerTable(*successor)
	return nil
}

func Stabilize() {
	var nD *NodeDetails
	successorIndex := -1
	node := Get()
	for position, item := range node.Successors {
		var err error
		nD, err = Predecessor(FetchChordAddress(item))
		log.Printf("predecessor is %v, err: %v, index: %v", nD, err, position)
		if err == nil {
			successorIndex = position
			break
		}
	}
	// If the successor does not point to a predecessor, then nD might be nil, hence checking for that is necessary.
	if nD != nil && Within(&node.Details.ID, &nD.ID, &node.Successors[successorIndex].ID, false) {
		UpdateSuccessor(*nD)
	} else if nD != nil {
		// new successor is made the first active successor from the previous list.
		UpdateSuccessor(node.Successors[successorIndex])
	} else {
		// If there are no successors, refer to yourself
		UpdateSuccessor(node.Details)
	}
	if len(nodeInstance.Successors) > 0 {
		err := RpcNotify(FetchChordAddress(Successor()), node.Details)
		if err != nil {
			log.Printf("Notification error with successor %v: %v", Successor(), err.Error())
		}

		successors, err := Successors(FetchChordAddress(Successor()))
		if err == nil {
			AddSuccessors(successors)
		} else {
			log.Printf("Error fetching successors from %v: %v", Successor(), err.Error())
		}
	}
	log.Printf("Successor list updated with new successor %v, new length: %v", node.Successors[0], len(node.Successors))
}

func Notify(node NodeDetails) {
	n := Get()
	Msg := fmt.Sprintf("Invoked by: %v with ID: %v, Current predecessor: %v", node.IPaddress, node.ID.String(), n.Predecessor)
	// To avoid getting stuck, make sure that it doesn't have itself as predecessor
	if n.Predecessor == nil || n.Predecessor.ID.Cmp(&n.Details.ID) == 0 ||
		Within(&n.Predecessor.ID, &node.ID, &n.Details.ID, false) {
		SetPredecessor(&node)
		Msg += ". updated Predecessor."
	} else {
		Msg += ". not updated Predecessor."
	}
	log.Printf("%v\n", Msg)
}

func FixFingers() {
	index := IncrementFollowFinger()
	log.Printf("next: %v", index)
	nI := Get()
	idToFix := *Jump(nI.Details.ID, index)
	node, err := FindNode(idToFix, nI.Details, 32)
	if err != nil {
		log.Printf("Error occured while setting finger table index %v to Id %v+2^(%v)=%v. err: %v\n", index, nI.Details.ID, index, idToFix, err.Error())
		return
	}

	if index >= nodeInstance.FingerTableSize || index > len(nodeInstance.FingerTable) {
		log.Printf("Index beyond size, element at position" + fmt.Sprintf("%v", index-1) + "missing")

	}
	if index < len(nodeInstance.FingerTable) {
		nodeInstance.FingerTable[index] = *node
		return
	}
	nodeInstance.FingerTable = append(nodeInstance.FingerTable, *node)
}

func CheckPredecessor() {
	node := Get()
	if node.Predecessor != nil && !IsAlive(FetchChordAddress(*node.Predecessor)) {
		SetPredecessor(nil)
		log.Printf(" Predecessor set to nil due to unresponsiveness from previous predecessor  %v\n", node.Predecessor)
	}
}

func HexStringToBytes(hexString string) (*big.Int, error) {
	bytes, err := hex.DecodeString(hexString)
	if err != nil {
		return nil, err
	}
	return new(big.Int).SetBytes(bytes), nil
}

const keyLength = RING_SIZE_BITS

var two = big.NewInt(2)
var hashMod = new(big.Int).Exp(two, big.NewInt(keyLength), nil)

func GenerateHash(elt string) *big.Int {
	hash := sha1.New()
	hash.Write([]byte(elt))
	return new(big.Int).SetBytes(hash.Sum(nil))
}

func Jump(nodeIdentifier big.Int, fingerentry int) *big.Int {
	fingerentryBig := big.NewInt(int64(fingerentry))
	jump := new(big.Int).Exp(two, fingerentryBig, nil)
	sum := new(big.Int).Add(&nodeIdentifier, jump)

	return new(big.Int).Mod(sum, hashMod)
}

func Within(start, elt, end *big.Int, inclusive bool) bool {
	if end.Cmp(start) > 0 {
		return (start.Cmp(elt) < 0 && elt.Cmp(end) < 0) || (inclusive && elt.Cmp(end) == 0)
	} else {
		return start.Cmp(elt) < 0 || elt.Cmp(end) < 0 || (inclusive && elt.Cmp(end) == 0)
	}
}

func InitializeNodeFileSystem(node_id string) error {
	folder := FetchFileLocation(node_id)
	err := os.MkdirAll(folder, DIR_PRIVILEGES)
	if err != nil {
		return err
	}
	return nil
}

func FileRead(fileLoc string) ([]byte, error) {
	file, err := os.ReadFile(fileLoc)
	return file, err
}

func FetchFileLocation(nodeId string) string {
	return filepath.Join(RESOURCES_FOLDER, nodeId)
}

func FetchFilePath(key, nodeKey string) string {
	return filepath.Join(FetchFileLocation(nodeKey), key)
}

func WriteNodeFile(key, node_Id string, data []byte) error {
	directory := FetchFileLocation(node_Id)
	err := os.MkdirAll(directory, DIR_PRIVILEGES)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(directory, key), data, FILE_PRIVILEGES)
}

func WriteNodeFiles(node_Id string, files map[string]*[]byte) []error {
	folder := FetchFileLocation(node_Id)
	err := os.MkdirAll(folder, DIR_PRIVILEGES)
	if err != nil {
		return []error{err}
	}
	Writeerrs := []error{}
	for key, data := range files {
		Writeerr := os.WriteFile(FetchFilePath(key, node_Id), *data, FILE_PRIVILEGES)
		if Writeerr != nil {
			Writeerrs = append(Writeerrs, Writeerr)
		}
	}
	return Writeerrs
}