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

// FetchNodeArrayState retrieves the state information of an array of Chord nodes.
// It returns a string containing the details of each node, including identifier, IP address, port, and secure port.
// The calculateIdealIdentifier function is used to compute the ideal identifier for each node based on its position in the array.
func FetchNodeArrayState(nodes []NodeDetails, calculateIdealIdentifier CalculateIdealIdentifier) (*string, error) {
	// Initialize an empty string to store the status information
	status := new(string)

	// Iterate through each node in the array
	for position, object := range nodes {
		// Calculate the ideal identifier for the current node
		idealIdentifier := calculateIdealIdentifier(position)

		// Fetch the state information for the current node
		info, err := FetchNodeState(object, true, position, &idealIdentifier)
		if err != nil {
			return nil, err
		}

		// Append the node's state information to the status string
		*status += *info + "\n\n"
	}

	return status, nil
}

// FetchState retrieves the detailed state information of the current Chord node.
// It returns a string containing information such as the node's identifier, IP address, port, secure port,
// predecessor details, successor details, and finger table entries.
func FetchState() (*string, error) {
	// Get the current Chord node
	node := Get()

	// Fetch the state information for the current node
	status, err := FetchNodeState(node.Details, false, -1, nil)
	if err != nil {
		return nil, err
	}

	// Append information about the predecessor to the status string
	*status += "Predecessor: "
	if node.Predecessor == nil {
		*status += "None \n"
	} else {
		*status += node.Predecessor.ID.String()
	}

	*status += "\n\nSuccessors:\n"

	// Fetch and append information about the successors to the status string
	successorStatus, err := FetchNodeArrayState(node.Successors, func(i int) big.Int {
		return *new(big.Int).Add(big.NewInt(int64(i+1)), &node.Details.ID)
	})
	if err != nil {
		return nil, err
	}
	if successorStatus == nil {
		*status += "No successors\n"
	} else {
		*status += *successorStatus
	}

	*status += "\nFinger table:\n"

	// Fetch and append information about the finger table entries to the status string
	fingerTableStatus, err := FetchNodeArrayState(node.FingerTable, func(i int) big.Int {
		return *Jump(node.Details.ID, i)
	})
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

// SearchSuccessor searches for the successor of the specified identifier in the Chord ring.
// It returns a boolean indicating whether the current Chord node is the successor,
// and the details of the successor node.
func SearchSuccessor(id big.Int) (bool, NodeDetails) {
	// Get the current Chord node
	node := Get()

	// Check if the identifier is within the range of the current Chord node's successors
	if Within(&node.Details.ID, &id, &node.Successors[0].ID, true) {
		log.Printf("Successor search Id: %v, result: %v, %v", id.String(), true, node.Successors[0])
		return true, node.Successors[0]
	}

	// Find the nearest preceding node to the given identifier
	nodeNearest := NearestPrecedingNode(id)
	log.Printf("Successor search Id: %v, result: %v, %v", id.String(), false, nodeNearest)
	return false, nodeNearest
}

// FindNode searches for the Chord node responsible for the specified identifier in the Chord ring.
// It starts the search from the given node details and takes a specified number of steps.
// It returns the details of the identified node or an error if the successor is not found.
func FindNode(id big.Int, start NodeDetails, Steps int) (*NodeDetails, error) {
	identified := false
	successorNode := start

	// Perform a series of steps to find the successor node
	for i := 0; i < Steps; i++ {
		// Call RPC to search for the successor of the given identifier
		res, err := RpcSearchSuccessor(FetchChordAddress(successorNode), &id)
		if err != nil {
			return nil, err
		}

		// Check if the successor is found
		identified = res.Found
		if identified {
			return &res.Node, nil
		}

		// Update the successor node for the next step
		successorNode = res.Node
	}

	// Return an error if the successor is not found after the specified number of steps
	return nil, errors.New("successor not found")
}

// FindNearPrecedingCandidate finds the nearest preceding node in the finger table of a Chord node.
// It takes the current Chord node, its finger table, and the target identifier as parameters.
// It returns a pointer to the nearest preceding node found in the finger table.
func FindNearPrecedingCandidate(n Node, table []NodeDetails, id big.Int) *NodeDetails {
	for i := len(table) - 1; i >= 0; i-- {
		// Check if the current finger table entry is a valid candidate
		if Within(&n.Details.ID, &table[i].ID, &id, false) {
			return &table[i]
		}
	}
	// Return nil if no valid candidate is found in the finger table
	return nil
}

// NearestPrecedingNode finds the nearest preceding node to the given identifier.
// It takes the target identifier as a parameter.
// It returns the NodeDetails of the nearest preceding node.
func NearestPrecedingNode(id big.Int) NodeDetails {
	node := Get()

	// Find the nearest preceding node in the FingerTable.
	var candidate *NodeDetails = FindNearPrecedingCandidate(node, node.FingerTable, id)

	// Check the Successors for a closer preceding node.
	if c := FindNearPrecedingCandidate(node, node.Successors, id); candidate == nil || (c != nil &&
		Within(&id, &c.ID, &candidate.ID, false)) {
		candidate = c
	}

	// Log the result.
	if candidate != nil {
		log.Printf("Near preceding node id: %v, result: %v\n", id, *candidate)
		return *candidate
	}

	// If no preceding node is found, return the current node's details.
	log.Printf("Near preceding node id: %v, result: %v\n", id, node.Details)
	return node.Details
}

// Begin initializes the Chord node. It sets up the node's details, initializes the node if creating a new ring, and joins an existing ring if specified.
// Parameters:
// - ownIp: IP address of the Chord node.
// - ownPort: Port number of the Chord node.
// - securePort: Secure port number of the Chord node.
// - fingerTableCount: Size of the Chord node's finger table.
// - successorsCount: Number of successors to be maintained by the Chord node.
// - initNewRing: A boolean indicating whether to initialize a new ring.
// - joinIp: IP address of the existing Chord node to join (required if initNewRing is false).
// - joinPort: Port number of the existing Chord node to join (required if initNewRing is false).
// - additionalId: An optional additional identifier for the Chord node.
// Returns an error if initialization or joining fails.
func Begin(ownIp string, ownPort, securePort, fingerTableCount, successorsCount int, initNewRing bool, joinIp *string, joinPort *int, additionalId *big.Int) error {
	log.Printf("Node started %v:%v with secure port: %v", ownIp, ownPort, securePort)

	// Initialize the Chord node's details.
	err := InitializeNode(ownIp, ownPort, securePort, fingerTableCount, successorsCount, additionalId)
	if err != nil {
		return err
	}

	// Create a new ring if specified.
	if initNewRing {
		CreateRing()
		return nil
	}

	// Join an existing ring if specified.
	if joinIp == nil || joinPort == nil {
		return errors.New("if initNewRing is set to false, join IP address and join port are required")
	}

	// Temporarily store the node's ID before joining.
	tempID := Get().Details.ID

	// Join the existing ring.
	return JoinRing(*joinIp, *joinPort, &tempID, fingerTableCount)
}

// CreateRing initializes a new Chord ring. It sets the Chord node as its own successor and updates the finger table accordingly.
func CreateRing() {
	// Set the Chord node as its own successor.
	UpdateSuccessor(Get().Details)

	// Update the finger table with the Chord node as the only entry.
	UpdateFingerTable(Get().Details)
}

// JoinRing joins the current Chord node to an existing Chord ring.
// It locates the appropriate position in the ring and updates the successor and finger table accordingly.
func JoinRing(joinIp string, joinPort int, nodeId *big.Int, maxSteps int) error {
	// Dummy values only for the method to work
	joinHash := big.NewInt(-1)
	securePort := -1

	// Find the successor node to join the ring.
	successor, err := FindNode(*nodeId, NodeDetails{IPaddress: joinIp, Port: joinPort, SecurePort: securePort, ID: *joinHash}, maxSteps)
	if err != nil {
		return err
	}

	// Update the successor and finger table based on the found successor node.
	UpdateSuccessor(*successor)
	UpdateFingerTable(*successor)

	return nil
}

// Stabilize performs the stabilization process for the Chord node.
// It checks and updates the successor node and fetches the latest list of successors from the current successor.
func Stabilize() {
	var nD *NodeDetails
	successorIndex := -1
	node := Get()

	// Iterate over the current list of successors to find a predecessor for stabilization.
	for position, item := range node.Successors {
		var err error
		nD, err = Predecessor(FetchChordAddress(item))
		log.Printf("predecessor is %v, err: %v, index: %v", nD, err, position)
		if err == nil {
			successorIndex = position
			break
		}
	}

	// If the successor does not point to a predecessor, then nD might be nil.
	// Check if nD is not nil and the predecessor is within the expected range.
	if nD != nil && Within(&node.Details.ID, &nD.ID, &node.Successors[successorIndex].ID, false) {
		UpdateSuccessor(*nD)
	} else if nD != nil {
		// If the predecessor is not within the range, make the first active successor from the previous list as the new successor.
		UpdateSuccessor(node.Successors[successorIndex])
	} else {
		// If there are no successors, refer to yourself.
		UpdateSuccessor(node.Details)
	}

	// Notify the successor about the update and fetch the latest list of successors.
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

// Notify is invoked by another Chord node to inform about its presence.
// It updates the predecessor of the current node if the incoming node is a suitable predecessor.
func Notify(node NodeDetails) {
	n := Get()
	Msg := fmt.Sprintf("Invoked by: %v with ID: %v, Current predecessor: %v", node.IPaddress, node.ID.String(), n.Predecessor)

	// To avoid getting stuck, make sure that it doesn't have itself as predecessor
	if n.Predecessor == nil || n.Predecessor.ID.Cmp(&n.Details.ID) == 0 ||
		Within(&n.Predecessor.ID, &node.ID, &n.Details.ID, false) {
		SetPredecessor(&node)
		Msg += ". Updated predecessor."
	} else {
		Msg += ". Not updated predecessor."
	}

	log.Printf("%v\n", Msg)
}

// FixFingers updates the finger table by setting the next finger at regular intervals.
// It calculates the next finger based on the current node's details and updates the finger table.
func FixFingers() {
	index := IncrementFollowFinger()
	log.Printf("Next finger index: %v", index)
	nI := Get()
	idToFix := *Jump(nI.Details.ID, index)

	node, err := FindNode(idToFix, nI.Details, 32)
	if err != nil {
		log.Printf("Error occurred while setting finger table index %v to Id %v+2^(%v)=%v. Error: %v\n", index, nI.Details.ID, index, idToFix, err.Error())
		return
	}

	// Check if the index is within bounds before updating the finger table.
	if index >= nodeInstance.FingerTableSize || index > len(nodeInstance.FingerTable) {
		log.Printf("Index beyond size, element at position %v missing", index-1)
		return
	}

	// Update the finger table at the calculated index.
	if index < len(nodeInstance.FingerTable) {
		nodeInstance.FingerTable[index] = *node
		return
	}

	// If the index is beyond the current finger table size, append the node to the finger table.
	nodeInstance.FingerTable = append(nodeInstance.FingerTable, *node)
}

// CheckPredecessor checks the liveness of the current node's predecessor.
// If the predecessor is unresponsive, it sets the predecessor to nil.
func CheckPredecessor() {
	node := Get()

	// Check if the predecessor is not nil and is unresponsive.
	if node.Predecessor != nil && !IsAlive(FetchChordAddress(*node.Predecessor)) {
		SetPredecessor(nil)
		log.Printf("Predecessor set to nil due to unresponsiveness from the previous predecessor %v\n", node.Predecessor)
	}
}

// HexStringToBytes converts a hexadecimal string to a big.Int.
// It uses the hex.DecodeString function to convert the hex string to a byte slice.
// The byte slice is then converted to a big.Int using new(big.Int).SetBytes.
// If there is an error during the decoding or conversion process, it returns an error.
func HexStringToBytes(hexString string) (*big.Int, error) {
	bytes, err := hex.DecodeString(hexString)
	if err != nil {
		return nil, err
	}
	return new(big.Int).SetBytes(bytes), nil
}

// keyLength represents the size of the Chord ring in bits.
const keyLength = RING_SIZE_BITS

// two is a big.Int constant with a value of 2.
var two = big.NewInt(2)

// hashMod is the result of 2^keyLength, representing the modulus for Chord hash values.
var hashMod = new(big.Int).Exp(two, big.NewInt(keyLength), nil)

// GenerateHash calculates the SHA-1 hash of the given string and returns it as a big.Int.
func GenerateHash(elt string) *big.Int {
	// Create a new SHA-1 hash instance
	hash := sha1.New()

	// Write the byte representation of the input string to the hash
	hash.Write([]byte(elt))

	// Return the hash result as a big.Int
	return new(big.Int).SetBytes(hash.Sum(nil))
}

// Jump calculates the target identifier by adding 2^fingerentry to the nodeIdentifier and taking the result modulo 2^(keyLength).
func Jump(nodeIdentifier big.Int, fingerentry int) *big.Int {
	// Convert fingerentry to a big.Int
	fingerentryBig := big.NewInt(int64(fingerentry))

	// Calculate the jump as 2^fingerentry
	jump := new(big.Int).Exp(two, fingerentryBig, nil)

	// Calculate the sum by adding nodeIdentifier and jump
	sum := new(big.Int).Add(&nodeIdentifier, jump)

	// Take the result modulo 2^(keyLength)
	return new(big.Int).Mod(sum, hashMod)
}

// Within checks if the given element is within the range [start, end] (inclusive or exclusive).
// Returns true if the element is within the range, false otherwise.
func Within(start, elt, end *big.Int, inclusive bool) bool {
	if end.Cmp(start) > 0 {
		// Case where end is greater than start
		return (start.Cmp(elt) < 0 && elt.Cmp(end) < 0) || (inclusive && elt.Cmp(end) == 0)
	} else {
		// Case where end is less than or equal to start (wrapping around the ring)
		return start.Cmp(elt) < 0 || elt.Cmp(end) < 0 || (inclusive && elt.Cmp(end) == 0)
	}
}

// InitializeNodeFileSystem creates the directory structure for the Chord node's file system.
// The directory structure is based on the provided node_id.
// Returns an error if the directory creation fails.
func InitializeNodeFileSystem(nodeID string) error {
	// Fetch the file location based on the node_id
	folder := FetchFileLocation(nodeID)

	// Create the directory structure with the specified permissions
	err := os.MkdirAll(folder, DIR_PRIVILEGES)
	if err != nil {
		return err
	}

	return nil
}

// FileRead reads the content of the file located at the specified fileLoc.
// It returns the content as a byte slice and any error encountered during the file read operation.
func FileRead(fileLoc string) ([]byte, error) {
	// Read the content of the file
	file, err := os.ReadFile(fileLoc)
	return file, err
}

// FetchFileLocation generates the file location for the Chord node identified by nodeId.
// The file location is determined based on the nodeId and the resources folder.
// It returns the complete file location as a string.
func FetchFileLocation(nodeId string) string {
	// Join the resources folder path with the nodeId to form the complete file location
	return filepath.Join(RESOURCES_FOLDER, nodeId)
}

// FetchFilePath generates the file path for a file identified by 'key' within the Chord node identified by 'nodeKey'.
// The file path is determined based on the key, nodeKey, and the resources folder.
// It returns the complete file path as a string.
func FetchFilePath(key, nodeKey string) string {
	// Join the file location for the Chord node with the key to form the complete file path
	return filepath.Join(FetchFileLocation(nodeKey), key)
}

// WriteNodeFile writes data to a file within the Chord node's file system.
// The file is identified by 'key' and belongs to the Chord node identified by 'nodeID'.
// It creates the necessary directory structure if not already present.
// Returns an error if any.
func WriteNodeFile(key, nodeID string, data []byte) error {
	// Fetch the directory location for the Chord node
	directory := FetchFileLocation(nodeID)

	// Create the necessary directory structure if not already present
	err := os.MkdirAll(directory, DIR_PRIVILEGES)
	if err != nil {
		return err
	}

	// Write data to the specified file with appropriate privileges
	return os.WriteFile(filepath.Join(directory, key), data, FILE_PRIVILEGES)
}

// WriteNodeFiles writes multiple files to the Chord node's file system.
// The files are specified in the 'files' map, where keys represent file names and values are the corresponding data.
// The files belong to the Chord node identified by 'nodeID'.
// It creates the necessary directory structure if not already present.
// Returns a slice of errors encountered during the write process.
func WriteNodeFiles(nodeID string, files map[string]*[]byte) []error {
	// Fetch the directory location for the Chord node
	folder := FetchFileLocation(nodeID)

	// Create the necessary directory structure if not already present
	err := os.MkdirAll(folder, DIR_PRIVILEGES)
	if err != nil {
		return []error{err}
	}

	// Initialize a slice to store errors encountered during file writes
	writeErrors := []error{}

	// Iterate over the files in the map and write each file to the node's file system
	for key, data := range files {
		writeErr := os.WriteFile(FetchFilePath(key, nodeID), *data, FILE_PRIVILEGES)
		if writeErr != nil {
			writeErrors = append(writeErrors, writeErr)
		}
	}

	// Return the slice of errors (if any)
	return writeErrors
}
