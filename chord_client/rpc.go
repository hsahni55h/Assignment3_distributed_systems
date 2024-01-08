package main

import (
	"errors"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/rpc"
)

type PredecessorData struct {
	Predecessor *NodeDetails
}

type SuccessorData struct {
	Successors []NodeDetails
}

type FindSuccessorData struct {
	Found bool
	Node  NodeDetails
}

type FileStorageArguments struct {
	Key  big.Int
	Data []byte
}

type FileTransferArguments struct {
	Files map[string]*[]byte
}

type RPCHandler int

func RegisterRPC(l *net.Listener) {
	handler := new(RPCHandler)
	rpc.Register(handler)
	rpc.HandleHTTP()
	go http.Serve(*l, nil /*http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { log.Printf("New request: \n%v\n", *r) })*/)
}

func handleCall[ArgT, RepT any](nodeAddress string, method string, args *ArgT, reply *RepT) error {
	client, err := rpc.DialHTTP("tcp", string(nodeAddress))
	if err != nil {
		return err
	}
	return client.Call(method, args, reply)
}

// predecessor of node is returned
func Predecessor(node string) (*NodeDetails, error) {
	var reply PredecessorData
	dummyArg := "empty"
	err := handleCall(node, "RPCHandler.Predecessor", &dummyArg, &reply)
	if err != nil {
		return nil, err
	}
	return reply.Predecessor, nil
}

// successors of node is returned
func Successors(node string) ([]NodeDetails, error) {
	var reply SuccessorData
	// Using a dummy string
	dummyArg := "empty"
	err := handleCall(node, "RPCHandler.Successors", &dummyArg, &reply)
	if err != nil {
		return nil, err
	}
	return reply.Successors, err
}

func RpcSearchSuccessor(node string, id *big.Int) (*FindSuccessorData, error) {
	var reply FindSuccessorData
	err := handleCall(node, "RPCHandler.SearchSuccessor", id, &reply)
	if err != nil {
		return nil, err
	}
	return &reply, nil
}

func RpcNotify(notifiee string, notifier NodeDetails) error {
	var reply string
	err := handleCall(notifiee, "RPCHandler.Notify", &notifier, &reply)
	return err
}

func SaveClientFile(nodeAddress string, fileKey big.Int, content []byte) error {
	var reply string
	args := FileStorageArguments{Key: fileKey, Data: content}
	err := handleCall(nodeAddress, "RPCHandler.StoreFile", &args, &reply)
	return err
}

func TransferFiles(nodeAddress string, files map[string]*[]byte) error {
	var reply string
	args := FileTransferArguments{Files: files}
	err := handleCall(nodeAddress, "RPCHandler.TransferFiles", &args, &reply)
	return err
}

func IsAlive(nodeAddress string) bool {
	dummy := "empty"
	var reply bool
	err := handleCall(nodeAddress, "RPCHandler.IsAlive", &dummy, &reply)
	return err == nil && reply
}

func (t *RPCHandler) Predecessor(empty string, reply *PredecessorData) error {
	n := Get()
	*reply = PredecessorData{Predecessor: n.Predecessor}
	return nil
}

func (t *RPCHandler) Successors(empty string, reply *SuccessorData) error {
	n := Get()
	*reply = SuccessorData{Successors: n.Successors}
	return nil
}

func (t *RPCHandler) SearchSuccessor(args *big.Int, reply *FindSuccessorData) error {
	f, n := SearchSuccessor(*args)
	*reply = FindSuccessorData{Found: f, Node: n}
	return nil
}

func (t *RPCHandler) Notify(args *NodeDetails, reply *string) error {
	Notify(*args)
	return nil
}

// Locates the node to store the file using the lookup function
// Then, it uploads the file after locating it.
func (t *RPCHandler) StoreFile(args *FileStorageArguments, reply *string) error {
	key := args.Key.String()
	log.Printf("saved file %v, data length %v", key, len(args.Data))
	node_Id := Get().Details.ID

	return WriteNodeFile(key, node_Id.String(), args.Data)
}

// When a node attempts to transfer a file to this node
func (t *RPCHandler) TransferFiles(args *FileTransferArguments, reply *string) error {
	log.Printf("Saved %v files", len(args.Files))
	node_Id := Get().Details.ID
	errs := WriteNodeFiles(node_Id.String(), args.Files)
	if len(errs) > 0 {
		return errors.New("failed to write the files")
	}
	return nil
}

// True is returned to indicate node is still alive.
func (t *RPCHandler) IsAlive(empty string, reply *bool) error {
	*reply = true
	return nil
}
