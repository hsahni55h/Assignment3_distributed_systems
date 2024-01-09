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

// RESOURCES_FOLDER represents the path to the folder containing resources.
const RESOURCES_FOLDER = "./resources"

// FILE_PRIVILEGES represents the file access privileges (in octal) for newly created files.
const FILE_PRIVILEGES = 0o600

// DIR_PRIVILEGES represents the directory access privileges (in octal) for newly created directories.
const DIR_PRIVILEGES = 0o700

// NodeDetails represents the details of a Chord node.
type NodeDetails struct {
	IPaddress  string
	Port       int
	SecurePort int
	ID         big.Int
}

// Node represents a Chord node in the distributed system.
type Node struct {
	Details         NodeDetails
	FingerTable     []NodeDetails
	FingerTableSize int
	Predecessor     *NodeDetails
	Successors      []NodeDetails
	SuccessorsSize  int
	NextFinger      int
}

// once is a synchronization mechanism used to ensure the singleton pattern for the Node instance.
var once sync.Once

// nodeInstance is a singleton instance of the Chord Node.
var nodeInstance Node

// InitializeNode initializes the Chord node with the provided parameters.
// It creates a singleton instance of the Chord node using the provided IP address, port, secure port, finger table count,
// successors count, and additional identifier (if provided).
// Returns an error if the sizes of finger table or successors are less than 1.
func InitializeNode(ownIp string, ownPort, securePort, fingerTableCount, successorsCount int, additionalId *big.Int) error {
	// Check if the sizes are valid
	if fingerTableCount < 1 || successorsCount < 1 {
		return errors.New("sizes need to be at least 1")
	}

	// Initialize the Chord node using the singleton pattern
	once.Do(func() {
		// Create the address string by concatenating IP and port
		var address = ownIp + ":" + fmt.Sprintf("%v", ownPort)
		var Details NodeDetails

		// Check if an additional identifier is provided
		if additionalId == nil {
			Details = NodeDetails{
				IPaddress:  ownIp,
				Port:       ownPort,
				SecurePort: securePort,
				ID:         *GenerateHash(address),
			}
		} else {
			Details = NodeDetails{
				IPaddress:  ownIp,
				Port:       ownPort,
				SecurePort: securePort,
				ID:         *additionalId,
			}
		}

		// Create the singleton instance of the Chord node
		nodeInstance = Node{
			Details:         Details,
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


// FetchChordAddress returns the formatted address string for the given Chord node details.
// It concatenates the IP address and port in the format "IP:Port".
func FetchChordAddress(node NodeDetails) string {
	return fmt.Sprintf("%v:%v", node.IPaddress, node.Port)
}


// FetchSshAddress returns the formatted SSH address string for the given Chord node details.
// It concatenates the IP address and SSH port in the format "IP:SSHPort".
func FetchSshAddress(node NodeDetails) string {
	return fmt.Sprintf("%v:%v", node.IPaddress, node.SecurePort)
}


// IncrementFollowFinger increments the NextFinger index in the Chord node's instance and returns the updated index.
// The index is rotated within the range [0, FingerTableSize) to follow the Chord finger table structure.
func IncrementFollowFinger() int {
	nodeInstance.NextFinger = (nodeInstance.NextFinger + 1) % nodeInstance.FingerTableSize
	return nodeInstance.NextFinger
}


// UpdateSuccessor updates the Chord node's successor with the provided NodeDetails.
// It replaces the existing successor list with a new list containing only the provided successor.
func UpdateSuccessor(successor NodeDetails) {
	nodeInstance.Successors = []NodeDetails{successor}
}


// UpdateFingerTable updates the Chord node's finger table with the provided successor NodeDetails.
// It replaces the existing finger table with a new table containing only the provided successor.
func UpdateFingerTable(successor NodeDetails) {
	nodeInstance.FingerTable = []NodeDetails{successor}
}


// CheckSuccessorsContainItself checks if the Chord node's own details are present in the provided successors.
// It returns the index of the Chord node's details in the slice if found, otherwise, it returns -1.
func CheckSuccessorsContainItself(successors []NodeDetails) int {
	for index, item := range successors {
		// Compare the ID of the Chord node with the ID of each successor in the provided slice.
		if item.ID.Cmp(&nodeInstance.Details.ID) == 0 {
			// If the Chord node's details are found in the successors, return the index.
			return index
		}
	}
	// If the Chord node's details are not found in the successors, return -1.
	return -1
}


// AddSuccessors appends the successor's successors to the node's list of successors.
func AddSuccessors(successors []NodeDetails) {
	// Calculate the maximum number of elements that can be added to the successors slice.
	elementsCount := nodeInstance.SuccessorsSize - 1

	var addElements []NodeDetails

	// Ensure that the number of elements to add does not exceed the maximum allowed.
	// To avoid panic
	if len(successors) > elementsCount {
		addElements = successors[:elementsCount]
	} else {
		addElements = successors
	}

	// Check if the Chord node's own details are present in the elements to add.
	index := CheckSuccessorsContainItself(addElements)
	if index != -1 {
		// If found, truncate the elements to add up to the index.
		addElements = addElements[:index]
	}

	// Append the elements to add to the node's list of successors.
	nodeInstance.Successors = append(nodeInstance.Successors, addElements...)
}


// Successor returns the first successor in the node's list of successors.
func Successor() NodeDetails {
	return nodeInstance.Successors[0]
}

// SetPredecessor sets the predecessor of the Chord node to the provided node details.
func SetPredecessor(predecessor *NodeDetails) {
	nodeInstance.Predecessor = predecessor
}


// Get returns the Chord node instance.
func Get() Node {
	return nodeInstance
}


// Lookup finds the Chord node responsible for the given file key.
// It returns the details of the responsible node or an error if the operation fails.
func Lookup(fileKey big.Int) (*NodeDetails, error) {
	node := Get()
	foundNode, err := FindNode(fileKey, node.Details, 32)
	if err != nil {
		return nil, err
	}
	return foundNode, nil
}


// StoreFile stores a file in the Chord DHT.
// It returns the details of the node where the file is stored, the identifier of the stored file, or an error if the operation fails.
func StoreFile(fileLoc string, ssh bool, encrypt bool) (*NodeDetails, *big.Int, error) {
	// Extract the filename from the file location
	Loc := strings.Split(fileLoc, "/")
	fileName := Loc[len(Loc)-1]

	// Generate a hash (identifier) for the file based on its name
	fileIdentifier := GenerateHash(fileName)

	// Look up the Chord node responsible for storing the file
	node, err := Lookup(*fileIdentifier)
	if err != nil {
		return nil, nil, err
	}

	// Read the content of the file
	data, err := FileRead(fileLoc)
	if err != nil {
		return nil, nil, err
	}

	// Encrypt the file data if encryption is enabled
	if encrypt {
		cipherData, err := EncryptData(data)
		if err != nil {
			return nil, nil, err
		}
		data = cipherData
	}

	// Log the attempt to store the file
	log.Printf("Attempting to store file at %v", *node)

	// Transmit the file using SSH if specified
	if ssh {
		fileName := fileIdentifier.String()
		err := TransmitFile(FetchSshAddress(*node), fileName, data)
		if err != nil {
			return nil, nil, err
		}
	} else {
		// Save the file to the Chord node
		err := SaveClientFile(FetchChordAddress(*node), *fileIdentifier, data)
		if err != nil {
			return nil, nil, err
		}
	}

	return node, fileIdentifier, nil
}


// FetchNodeState retrieves the state information of a Chord node.
// It returns a string containing the node's identifier, IP address, port, and secure port.
// If collectionItem is true, it includes additional information such as the index and ideal identifier.
func FetchNodeState(node NodeDetails, collectionItem bool, index int, idealIdentifier *big.Int) (*string, error) {
	// Format the basic node details
	NodeDetails := fmt.Sprintf("Identifier: %v address: %v:%v SecurePort: %v", node.ID.String(), node.IPaddress, node.Port, node.SecurePort)

	// Include additional information if specified
	if collectionItem {
		NodeDetails += fmt.Sprintf("\nIndex: %v\nIdeal Identifier: %v", index, idealIdentifier)
	}

	// Append a newline character
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
