package main

import (
	"errors"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/rpc"
)

// PredecessorData represents the data structure for the predecessor of a Chord node.
type PredecessorData struct {
	Predecessor *NodeDetails
}

// SuccessorData represents the data structure for the successors of a Chord node.
type SuccessorData struct {
	Successors []NodeDetails
}

// FindSuccessorData represents the data structure for the result of a successor search.
type FindSuccessorData struct {
	Found bool
	Node  NodeDetails
}

// FileStorageArguments represents the arguments for storing a file in Chord.
type FileStorageArguments struct {
	Key  big.Int
	Data []byte
}

// FileTransferArguments represents the arguments for transferring files between Chord nodes.
type FileTransferArguments struct {
	Files map[string]*[]byte
}

// RPCHandler is the RPC handler for Chord node communication.
type RPCHandler int

// RegisterRPC registers the RPC handler and starts the HTTP server.
func RegisterRPC(l *net.Listener) {
	// Create a new RPCHandler
	handler := new(RPCHandler)

	// Register the RPC handler
	rpc.Register(handler)

	// Handle HTTP requests
	rpc.HandleHTTP()

	// Start the HTTP server
	go http.Serve(*l, nil)
}

// handleCall is a utility function for making RPC calls.
func handleCall[ArgT, RepT any](nodeAddress string, method string, args *ArgT, reply *RepT) error {
	// Establish an RPC connection
	client, err := rpc.DialHTTP("tcp", string(nodeAddress))
	if err != nil {
		return err
	}

	// Make the RPC call
	return client.Call(method, args, reply)
}

// Predecessor returns the predecessor of the specified Chord node.
func Predecessor(node string) (*NodeDetails, error) {
	var reply PredecessorData
	// Prepare the request
	dummyArg := "empty"

	// Make the RPC call
	err := handleCall(node, "RPCHandler.Predecessor", &dummyArg, &reply)
	if err != nil {
		return nil, err
	}

	// Return the predecessor
	return reply.Predecessor, nil
}

// Successors returns the successors of the specified Chord node.
func Successors(node string) ([]NodeDetails, error) {
	var reply SuccessorData
	// Prepare the request
	dummyArg := "empty"

	// Make the RPC call
	err := handleCall(node, "RPCHandler.Successors", &dummyArg, &reply)
	if err != nil {
		return nil, err
	}

	// Return the successors
	return reply.Successors, err
}

// RpcSearchSuccessor searches for the successor of a Chord node.
func RpcSearchSuccessor(node string, id *big.Int) (*FindSuccessorData, error) {
	// Prepare the request
	var reply FindSuccessorData

	// Make the RPC call
	err := handleCall(node, "RPCHandler.SearchSuccessor", id, &reply)
	if err != nil {
		return nil, err
	}

	// Return the result
	return &reply, nil
}

// RpcNotify notifies a Chord node.
func RpcNotify(notifiee string, notifier NodeDetails) error {
	// Prepare the request
	var reply string

	// Make the RPC call
	err := handleCall(notifiee, "RPCHandler.Notify", &notifier, &reply)
	return err
}

// SaveClientFile saves a file on a Chord node.
func SaveClientFile(nodeAddress string, fileKey big.Int, content []byte) error {
	// Prepare the request
	var reply string
	args := FileStorageArguments{Key: fileKey, Data: content}

	// Make the RPC call
	err := handleCall(nodeAddress, "RPCHandler.StoreFile", &args, &reply)
	return err
}

// TransferFiles transfers files between Chord nodes.
func TransferFiles(nodeAddress string, files map[string]*[]byte) error {
	// Prepare the request
	var reply string
	args := FileTransferArguments{Files: files}

	// Make the RPC call
	err := handleCall(nodeAddress, "RPCHandler.TransferFiles", &args, &reply)
	return err
}

// IsAlive checks if a Chord node is still alive.
func IsAlive(nodeAddress string) bool {
	// Prepare the request
	dummy := "empty"
	var reply bool

	// Make the RPC call
	err := handleCall(nodeAddress, "RPCHandler.IsAlive", &dummy, &reply)
	return err == nil && reply
}

// Predecessor handles the RPC call to get the predecessor of the current node.
func (t *RPCHandler) Predecessor(empty string, reply *PredecessorData) error {
	n := Get() // Assuming Get() is a function to retrieve node details.
	*reply = PredecessorData{Predecessor: n.Predecessor}
	return nil
}

// Successors handles the RPC call to get the successors of the current node.
func (t *RPCHandler) Successors(empty string, reply *SuccessorData) error {
	n := Get() // Assuming Get() is a function to retrieve node details.
	*reply = SuccessorData{Successors: n.Successors}
	return nil
}

// SearchSuccessor handles the RPC call to search for the successor of a given ID.
func (t *RPCHandler) SearchSuccessor(args *big.Int, reply *FindSuccessorData) error {
	f, n := SearchSuccessor(*args) // Assuming SearchSuccessor is implemented somewhere.
	*reply = FindSuccessorData{Found: f, Node: n}
	return nil
}

// Notify handles the RPC call to notify a Chord node.
func (t *RPCHandler) Notify(args *NodeDetails, reply *string) error {
	Notify(*args) // Assuming Notify is implemented somewhere.
	return nil
}

// StoreFile handles the RPC call to store a file on a Chord node.
func (t *RPCHandler) StoreFile(args *FileStorageArguments, reply *string) error {
	key := args.Key.String()
	log.Printf("saved file %v, data length %v", key, len(args.Data))
	nodeID := Get().Details.ID

	return WriteNodeFile(key, nodeID.String(), args.Data) // Assuming WriteNodeFile is implemented somewhere.
}

// TransferFiles handles the RPC call to transfer files between Chord nodes.
func (t *RPCHandler) TransferFiles(args *FileTransferArguments, reply *string) error {
	log.Printf("Saved %v files", len(args.Files))
	nodeID := Get().Details.ID
	errs := WriteNodeFiles(nodeID.String(), args.Files) // Assuming WriteNodeFiles is implemented somewhere.
	if len(errs) > 0 {
		return errors.New("failed to write the files")
	}
	return nil
}

// IsAlive handles the RPC call to check if a Chord node is still alive.
func (t *RPCHandler) IsAlive(empty string, reply *bool) error {
	*reply = true
	return nil
}
