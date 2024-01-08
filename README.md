# Assignments_Distributed_systems
Solution of Assignment3 Distributed systems


Clone the Repository

Run the following commands

# Build the ChordClient
$ go build .

# Run ChordClient with specified parameters
$ ./ChordClient -a 128.8.126.63 -p 4170 --ts 3000 --tff 1000 --tcp 3000 -r 4

# Run another instance of ChordClient with additional parameters
$ ./ChordClient -a localhost -p 4171 --ja localhost --jp 4170 --ts 3000 --tff 1000 --tcp 3000 -r 4

