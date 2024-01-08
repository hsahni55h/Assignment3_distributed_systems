package main

import (
	"bufio"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"math/big"
	"net"
	"os"
	"strings"
	"time"
)

const RING_SIZE_BITS = 160

type ChordFlags struct {
	LocalIp					string
	LocalPort             	int
	SecurePort				int
	JoinNodeIP             	string
	JoinNodePort           	int
	StabilizeInterval      	int
	FixFingersInterval     	int
	CheckPredInterval 		int
	BackupInterval         	int
	NumSuccessors      		int
	IDOverride           	string
}

type Command struct {
	requiredParams 			int
	optionalParams 			int
	usageString    			string
}

const (
	INVALID_STRING = "INVALID"
	INVALID_INT    = -1
)

func main() {
	var f ChordFlags
	err := ParseFlags(&f)
	if err != nil {
		log.Println("error occured while reading flags: " + err.Error())
		return
	}

	logFile := fmt.Sprintf("log%v.txt", f.LocalPort)
	file, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, fs.FileMode.Perm(0o600))
	if err != nil {
		log.Println("log file creation failed")
		return
	}
	defer file.Close()
	var errTrunc = os.Truncate(logFile, 0)
	if errTrunc != nil {
		log.Println("log file creation failed")
		return
	}
	log.SetOutput(file)

	checkInitNewRing := f.CheckInitializeRing()
	overrideID := f.GetOverrideId()
	var overrideIDBigInt *big.Int = nil
	if overrideID != nil {
		res, err := HexStringToBytes(*overrideID)
		if err != nil {
			log.Println("error while creating additional identifier: " + err.Error())
			return
		}
		overrideIDBigInt = res
	}

	errBegin := Begin(f.LocalIp, f.LocalPort, f.SecurePort, RING_SIZE_BITS, f.NumSuccessors, checkInitNewRing, &f.JoinNodeIP, &f.JoinNodePort, overrideIDBigInt)
	if errBegin != nil {
		log.Println("error while intializing node: " + errBegin.Error())
		return
	}

	nodeId := Get().Details.ID
	fmt.Println("Current node ID:", nodeId.String())
	InitializeNodeFileSystem(nodeId.String())

	listen, err := net.Listen("tcp", ":"+fmt.Sprintf("%v", f.LocalPort))
	if err != nil {
		log.Println("error when initializing the listening socket " + err.Error())
		return
	}
	RegisterRPC(&listen)

	// background tasks
	Schedule(Stabilize, time.Duration(f.StabilizeInterval*int(time.Millisecond)))
	Schedule(FixFingers, time.Duration(f.FixFingersInterval*int(time.Millisecond)))
	Schedule(CheckPredecessor, time.Duration(f.CheckPredInterval*int(time.Millisecond)))

	RunCommands()
}

func ParseFlags(f *ChordFlags) error {
	flag.StringVar(&f.LocalIp, "a", INVALID_STRING, "The IP address that the Chord client will bind to, as well as advertise to other nodes. Represented as an ASCII string (e.g., 128.8.126.63). Must be specified.")
	flag.IntVar(&f.LocalPort, "p", INVALID_INT, "The port that the Chord client will bind to and listen on. Represented as a base-10 integer. Must be specified.")
	//flag.IntVar(&f.SecurePort, "sp", INVALID_INT, "The ssh port that the Chord client will bind to and listen on. Represented as a base-10 integer. Optional.")
	flag.StringVar(&f.JoinNodeIP, "ja", INVALID_STRING, "The IP address of the machine running a Chord node. The Chord client will join this nodes ring. Represented as an ASCII string (e.g., 128.8.126.63). Must be specified if --jp is specified.")
	flag.IntVar(&f.JoinNodePort, "jp", INVALID_INT, "The port that an existing Chord node is bound to and listening on. The Chord client will join this nodes ring. Represented as a base-10 integer. Must be specified if --ja is specified.")
	flag.IntVar(&f.StabilizeInterval, "ts", INVALID_INT, "The time in milliseconds between invocations of ‘stabilize’. Represented as a base-10 integer. Must be specified, with a value in the range of [1,60000].")
	flag.IntVar(&f.FixFingersInterval, "tff", INVALID_INT, "The time in milliseconds between invocations of ‘fix fingers’. Represented as a base-10 integer. Must be specified, with a value in the range of [1,60000].")
	flag.IntVar(&f.CheckPredInterval, "tcp", INVALID_INT, "The time in milliseconds between invocations of ‘check predecessor’.	Represented as a base-10 integer. Must be specified, with a value in the range of [1,60000].")
	flag.IntVar(&f.NumSuccessors, "r", INVALID_INT, "The number of successors maintained by the Chord client. Represented as a base-10 integer. Must be specified, with a value in the range of [1,32].")
	flag.StringVar(&f.IDOverride, "i", INVALID_STRING, "The identifier (ID) assigned to the Chord client which will override the ID computed by the SHA1 sum of the clients IP address and port number. Represented as a string of 40 characters matching [0-9a-fA-F]. Optional parameter.")
	flag.Parse()
	return validateFlags(f)
}

func withinRange(f, startRange, endRange int) bool {
	return startRange <= f && f <= endRange
}

func errorMessage(flagname, description string) string {
	return fmt.Sprintf("please set %v: %v\n", flagname, description)
}

func validateFlags(f *ChordFlags) error {
	var errorString = ""
	if f.LocalIp == INVALID_STRING {
		errorString += errorMessage("-a", "ASCII string of ip address to bind chord client to")
	}
	if f.LocalPort == INVALID_INT {
		errorString += errorMessage("-p", "port number that the chord client listens on")
	}
	if f.SecurePort == INVALID_INT {
		errorString += errorMessage("-sp", "port that the chord client's ssh server is listening on")
	}
	if (f.JoinNodeIP == INVALID_STRING && f.JoinNodePort != INVALID_INT) || (f.JoinNodeIP != INVALID_STRING && f.JoinNodePort == INVALID_INT) {
		var flagname string
		if f.JoinNodeIP == INVALID_STRING {
			flagname = "--ja"
		} else {
			flagname = "--jp"
		}
		errorString += errorMessage(flagname, "If either —ja (join address) or —jp (join port) is used, both must be given.")
	}
	if !withinRange(f.StabilizeInterval, 1, 60000) {
		errorString += errorMessage("--ts", "Runtime for the stabilize call in milliseconds, in the range [1, 60000]")
	}
	if !withinRange(f.FixFingersInterval, 1, 60000) {
		errorString += errorMessage("--tff", "Runtime for fix fingers call in milliseconds, range [1, 60000]")
	}
	if !withinRange(f.CheckPredInterval, 1, 60000) {
		errorString += errorMessage("--tcp", "Runtime for predecessor call in milliseconds, in the range [1, 60000]")
	}
	if !withinRange(f.NumSuccessors, 1, 32) {
		errorString += errorMessage("-r", "Range of the number of successors [1, 32]")
	}
	if f.IDOverride != INVALID_STRING {
		var noOfChars = RING_SIZE_BITS / 4
		var _, err = hex.DecodeString(f.IDOverride)
		if err != nil || noOfChars != len(f.IDOverride) {
			errorString += errorMessage("-i", fmt.Sprintf("chord-provided hexadecimal override node identification, values: [0-9][a-f][A-F], total values: %v", noOfChars))
		}
	}
	if errorString == "" {
		return nil
	}
	return errors.New(errorString)
}

func (flag ChordFlags) GetOverrideId() *string {
	if flag.IDOverride == INVALID_STRING {
		return nil
	}
	return &flag.IDOverride
}

// Intialize ring if join address and joinport are not provided
func (flag ChordFlags) CheckInitializeRing() bool {
	return flag.JoinNodeIP == INVALID_STRING && flag.JoinNodePort == INVALID_INT
}

func FetchCommands() map[string]Command {
	return map[string]Command{
		"Lookup":     {1, 0, "usage: Lookup <filename>"},
		"StoreFile":  {1, 2, "usage: StoreFile <filepathOnDisk> [ssh: default=false, t or true to enable] encrypt file: default=false, t or true to enable]"},
		"PrintState": {0, 0, "usage: PrintState"},
	}
}

func verifyCommand(cmdArgs []string) error {
	if len(cmdArgs) <= 0 {
		return errors.New("please provide a command as an input")
	}
	cmd, ok := FetchCommands()[cmdArgs[0]]
	if !ok {
		return errors.New("command " + cmdArgs[0] + " does not exists")
	}

	// first arg is always the command
	if len(cmdArgs)-1 < cmd.requiredParams || len(cmdArgs)-1 > cmd.optionalParams+cmd.requiredParams {
		return errors.New(cmd.usageString)
	}
	return nil
}

func getTurnOffOption(cmdArr []string, index int) bool {
	if len(cmdArr) > index && (strings.ToLower(cmdArr[index]) == "true" || strings.ToLower(cmdArr[index]) == "t") {
		return true
	}
	return false
}

func executeCommand(cmdArr []string) {
	switch cmdArr[0] {
	case "Lookup":
		fileId := *GenerateHash(cmdArr[1])
		fmt.Println("FileID: ", fileId.String())
		ans, err := Lookup(fileId)
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		status, err := FetchNodeState(*ans, false, -1, nil)
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		fmt.Println(*status)
	case "StoreFile":
		ssh := getTurnOffOption(cmdArr, 2)
		encryption := getTurnOffOption(cmdArr, 3)
		node, file_Id, errStore := StoreFile(cmdArr[1], ssh, encryption)
		if errStore != nil {
			fmt.Println(errStore.Error())
			return
		}
		status, err := FetchNodeState(*node, false, -1, nil)
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		fmt.Println("Stored file successfully")
		fmt.Printf("FileId: %v\nStored at:\n%v\n", file_Id.String(), *status)
	case "PrintState":
		PrintState, err := FetchState()
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		fmt.Println(*PrintState)
	default:
		fmt.Println("command not Found")
	}
}

func RunCommands() {
	var scanner = bufio.NewReader(os.Stdin)
	for {
		fmt.Print("Chord client: ")
		args, err := scanner.ReadString('\n')
		if err != nil {
			fmt.Println("Type command in a single line.")
			continue
		}
		cmdArgs := strings.Fields(args)
		var errVerify = verifyCommand(cmdArgs)
		if errVerify != nil {
			fmt.Println(errVerify.Error())
			continue
		}
		executeCommand(cmdArgs)
	}
}

func Schedule(function func(), t time.Duration) {
	go func() {
		for {
			time.Sleep(t)
			function()
		}
	}()
}
