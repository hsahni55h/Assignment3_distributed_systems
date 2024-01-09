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

// RING_SIZE_BITS represents the number of bits used for the Chord ring.
const RING_SIZE_BITS = 160

// ChordFlags holds the command-line flags for the Chord client.
type ChordFlags struct {
	LocalIp            string
	LocalPort          int
	SecurePort         int
	JoinNodeIP         string
	JoinNodePort       int
	StabilizeInterval  int
	FixFingersInterval int
	CheckPredInterval  int
	BackupInterval     int
	NumSuccessors      int
	IDOverride         string
}

// Command represents a command along with its required and optional parameters.
type Command struct {
	requiredParams int
	optionalParams int
	usageString    string
}

// Constants for invalid string and integer values.
const (
	INVALID_STRING = "INVALID"
	INVALID_INT    = -1
)

// main is the entry point of the Chord client.
func main() {
	var f ChordFlags

	// Parse command-line flags
	err := ParseFlags(&f)
	if err != nil {
		log.Println("Error parsing command-line flags: " + err.Error())
		return
	}

	// Setup logging to a file
	logFile := fmt.Sprintf("log%v.txt", f.LocalPort)
	file, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, fs.FileMode.Perm(0o600))
	if err != nil {
		log.Println("Failed to create log file")
		return
	}
	defer file.Close()

	// Clear the log file
	var errTrunc = os.Truncate(logFile, 0)
	if errTrunc != nil {
		log.Println("Failed to truncate log file")
		return
	}
	log.SetOutput(file)

	// Check if initializing a new ring is required
	checkInitNewRing := f.CheckInitializeRing()

	// Handle optional override ID
	overrideID := f.GetOverrideId()
	var overrideIDBigInt *big.Int = nil
	if overrideID != nil {
		res, err := HexStringToBytes(*overrideID)
		if err != nil {
			log.Println("Error creating additional identifier: " + err.Error())
			return
		}
		overrideIDBigInt = res
	}

	// Initialize the Chord node
	errBegin := Begin(f.LocalIp, f.LocalPort, f.SecurePort, RING_SIZE_BITS, f.NumSuccessors, checkInitNewRing, &f.JoinNodeIP, &f.JoinNodePort, overrideIDBigInt)
	if errBegin != nil {
		log.Println("Error initializing node: " + errBegin.Error())
		return
	}

	// Get the current node ID and display it
	nodeId := Get().Details.ID
	fmt.Println("Current node ID:", nodeId.String())

	// Initialize the node's file system
	InitializeNodeFileSystem(nodeId.String())

	// Setup RPC listener
	listen, err := net.Listen("tcp", ":"+fmt.Sprintf("%v", f.LocalPort))
	if err != nil {
		log.Println("Error initializing the listening socket: " + err.Error())
		return
	}
	RegisterRPC(&listen)

	// Schedule background tasks
	Schedule(Stabilize, time.Duration(f.StabilizeInterval*int(time.Millisecond)))
	Schedule(FixFingers, time.Duration(f.FixFingersInterval*int(time.Millisecond)))
	Schedule(CheckPredecessor, time.Duration(f.CheckPredInterval*int(time.Millisecond)))

	// Run the interactive command-line interface
	RunCommands()
}


// ParseFlags reads and parses the command-line flags for the Chord client.
// It populates the provided ChordFlags structure with the parsed values.
// An error is returned if the flags are invalid or not specified.
func ParseFlags(f *ChordFlags) error {
	// Define command-line flags and their descriptions
	flag.StringVar(&f.LocalIp, "a", INVALID_STRING, "IP address for Chord client binding and advertisement. Must be specified as an ASCII string (e.g., 128.8.126.63).")
	flag.IntVar(&f.LocalPort, "p", INVALID_INT, "Port for Chord client binding and listening. Must be specified as a base-10 integer.")
	flag.StringVar(&f.JoinNodeIP, "ja", INVALID_STRING, "IP address of a Chord node for joining its ring. Must be specified if --jp is used.")
	flag.IntVar(&f.JoinNodePort, "jp", INVALID_INT, "Port of an existing Chord node for joining its ring. Must be specified if --ja is used.")
	flag.IntVar(&f.StabilizeInterval, "ts", INVALID_INT, "Time in milliseconds between 'stabilize' invocations. Must be specified in the range [1,60000].")
	flag.IntVar(&f.FixFingersInterval, "tff", INVALID_INT, "Time in milliseconds between 'fix fingers' invocations. Must be specified in the range [1,60000].")
	flag.IntVar(&f.CheckPredInterval, "tcp", INVALID_INT, "Time in milliseconds between 'check predecessor' invocations. Must be specified in the range [1,60000].")
	flag.IntVar(&f.NumSuccessors, "r", INVALID_INT, "Number of successors maintained by the Chord client. Must be specified in the range [1,32].")
	flag.StringVar(&f.IDOverride, "i", INVALID_STRING, "Identifier (ID) assigned to the Chord client, overriding the ID computed by the SHA1 sum of the client's IP address and port number. Must be a string of 40 characters matching [0-9a-fA-F]. Optional parameter.")

	// Parse the command-line flags
	flag.Parse()

	// Validate the parsed flags
	return validateFlags(f)
}

// withinRange checks if a given value is within the specified range [startRange, endRange].
func withinRange(value, startRange, endRange int) bool {
	// Check if the value falls within the specified range (inclusive).
	return startRange <= value && value <= endRange
}

// errorMessage generates an error message for a missing or invalid flag.
// Format: "Please set <flagname>: <description>"
func errorMessage(flagname, description string) string {
	return fmt.Sprintf("Please set %v: %v\n", flagname, description)
}

// validateFlags checks if the parsed ChordFlags structure has valid values.
// Returns an error if the flags are invalid.
func validateFlags(f *ChordFlags) error {
	var errorString strings.Builder

	// Validate LocalIp
	if f.LocalIp == INVALID_STRING {
		errorString.WriteString(errorMessage("-a", "Please specify the IP address to bind the Chord client to."))
	}

	// Validate LocalPort
	if f.LocalPort == INVALID_INT {
		errorString.WriteString(errorMessage("-p", "Please specify the port number that the Chord client listens on."))
	}

	// Validate SecurePort
	if f.SecurePort == INVALID_INT {
		errorString.WriteString(errorMessage("-sp", "Please specify the port that the Chord client's SSH server is listening on."))
	}

	// Validate JoinNodeIP and JoinNodePort
	if (f.JoinNodeIP == INVALID_STRING && f.JoinNodePort != INVALID_INT) || (f.JoinNodeIP != INVALID_STRING && f.JoinNodePort == INVALID_INT) {
		var flagname string
		if f.JoinNodeIP == INVALID_STRING {
			flagname = "--ja"
		} else {
			flagname = "--jp"
		}
		errorString.WriteString(errorMessage(flagname, "If either --ja (join address) or --jp (join port) is used, both must be given."))
	}

	// Validate StabilizeInterval
	if !withinRange(f.StabilizeInterval, 1, 60000) {
		errorString.WriteString(errorMessage("--ts", "Stabilize interval must be in the range [1, 60000] milliseconds."))
	}

	// Validate FixFingersInterval
	if !withinRange(f.FixFingersInterval, 1, 60000) {
		errorString.WriteString(errorMessage("--tff", "Fix fingers interval must be in the range [1, 60000] milliseconds."))
	}

	// Validate CheckPredInterval
	if !withinRange(f.CheckPredInterval, 1, 60000) {
		errorString.WriteString(errorMessage("--tcp", "Check predecessor interval must be in the range [1, 60000] milliseconds."))
	}

	// Validate NumSuccessors
	if !withinRange(f.NumSuccessors, 1, 32) {
		errorString.WriteString(errorMessage("-r", "Number of successors must be in the range [1, 32]."))
	}

	// Validate IDOverride
	if f.IDOverride != INVALID_STRING {
		var noOfChars = RING_SIZE_BITS / 4
		var _, err = hex.DecodeString(f.IDOverride)
		if err != nil || noOfChars != len(f.IDOverride) {
			errorString.WriteString(errorMessage("-i", fmt.Sprintf("Chord-provided hexadecimal override node identification should have %v characters [0-9a-fA-F].", noOfChars)))
		}
	}

	// Return an error if any validation checks fail
	if errorString.Len() == 0 {
		return nil
	}
	return errors.New(errorString.String())
}

// GetOverrideId returns the override ID provided in ChordFlags, or nil if not set.
func (flag ChordFlags) GetOverrideId() *string {
	// Check if the IDOverride is set to the invalid string.
	if flag.IDOverride == INVALID_STRING {
		return nil
	}
	// Return a pointer to the IDOverride.
	return &flag.IDOverride
}

// CheckInitializeRing returns true if both join address and join port are not provided,
// indicating the initialization of a new Chord ring.
func (flag ChordFlags) CheckInitializeRing() bool {
	// Check if join address and join port are set to their default values.
	return flag.JoinNodeIP == INVALID_STRING && flag.JoinNodePort == INVALID_INT
}

// FetchCommands returns a map of available commands with their respective Command structures.
func FetchCommands() map[string]Command {
	// Define a map of available commands with their Command structures.
	commands := map[string]Command{
		"Lookup":     {1, 0, "usage: Lookup <filename>"},
		"StoreFile":  {1, 2, "usage: StoreFile <filepathOnDisk> [ssh: default=false, t or true to enable] encrypt file: default=false, t or true to enable]"},
		"PrintState": {0, 0, "usage: PrintState"},
	}

	// Return the map of commands.
	return commands
}

// verifyCommand checks the validity of the given command arguments.
func verifyCommand(cmdArgs []string) error {
	// Check if the number of command arguments is less than or equal to 0.
	if len(cmdArgs) <= 0 {
		return errors.New("please provide a command as an input")
	}

	// Fetch the available commands and their structures.
	commands := FetchCommands()

	// Retrieve the command structure for the specified command.
	cmd, ok := commands[cmdArgs[0]]
	if !ok {
		// Return an error if the specified command does not exist.
		return errors.New("command " + cmdArgs[0] + " does not exist")
	}

	// The first argument is always the command.
	// Check if the number of provided parameters is within the valid range.
	if len(cmdArgs)-1 < cmd.requiredParams || len(cmdArgs)-1 > cmd.optionalParams+cmd.requiredParams {
		return errors.New(cmd.usageString)
	}

	// The command arguments are valid.
	return nil
}

// getTurnOffOption checks if the specified option in cmdArr is set to true.
func getTurnOffOption(cmdArr []string, index int) bool {
	// Check if there are enough elements in cmdArr and the specified option is set to true.
	if len(cmdArr) > index && (strings.ToLower(cmdArr[index]) == "true" || strings.ToLower(cmdArr[index]) == "t") {
		// Return true if the option is set to true.
		return true
	}
	// Return false if the option is not set to true or if there are not enough elements in cmdArr.
	return false
}

// executeCommand executes the specified command based on cmdArr.
func executeCommand(cmdArr []string) {
	switch cmd := cmdArr[0]; cmd {
	case "Lookup":
		// Generate a file ID based on the provided filename.
		fileID := *GenerateHash(cmdArr[1])
		fmt.Println("FileID:", fileID.String())

		// Perform a lookup for the file ID in the Chord network.
		if ans, err := Lookup(fileID); err != nil {
			fmt.Println(err.Error())
		} else if status, err := FetchNodeState(*ans, false, -1, nil); err != nil {
			fmt.Println(err.Error())
		} else {
			fmt.Println(*status)
		}

	case "StoreFile":
		// Check if SSH and encryption options are enabled.
		ssh, encryption := getTurnOffOption(cmdArr, 2), getTurnOffOption(cmdArr, 3)

		// Store the file and obtain the corresponding node and file ID.
		if node, fileID, err := StoreFile(cmdArr[1], ssh, encryption); err != nil {
			fmt.Println(err.Error())
		} else if status, err := FetchNodeState(*node, false, -1, nil); err != nil {
			fmt.Println(err.Error())
		} else {
			fmt.Println("Stored file successfully")
			fmt.Printf("FileID: %v\nStored at:\n%v\n", fileID.String(), *status)
		}

	case "PrintState":
		// Fetch and print the overall state of the Chord network.
		if PrintState, err := FetchState(); err != nil {
			fmt.Println(err.Error())
		} else {
			fmt.Println(*PrintState)
		}

	default:
		fmt.Println("Command not found")
	}
}

// RunCommands continuously prompts the user for Chord client commands and executes them.
func RunCommands() {
	// Initialize a scanner to read user input from the command line.
	scanner := bufio.NewReader(os.Stdin)

	// Continuously prompt the user for commands and execute them.
	for {
		fmt.Print("Chord client: ")

		// Read the user input until a newline character is encountered.
		args, err := scanner.ReadString('\n')
		if err != nil {
			fmt.Println("Type command in a single line.")
			continue
		}

		// Tokenize the input into command arguments.
		cmdArgs := strings.Fields(args)

		// Verify the validity of the command arguments.
		if errVerify := verifyCommand(cmdArgs); errVerify != nil {
			fmt.Println(errVerify.Error())
			continue
		}

		// Execute the command with the provided arguments.
		executeCommand(cmdArgs)
	}
}

// Schedule runs the given function in a goroutine at regular intervals.
func Schedule(function func(), interval time.Duration) {
	// Start a new goroutine to run the provided function at regular intervals.
	go func() {
		for {
			// Sleep for the specified interval before executing the function again.
			time.Sleep(interval)
			// Call the provided function.
			function()
		}
	}()
}
