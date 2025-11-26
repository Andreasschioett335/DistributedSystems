HOW TO RUN APLICATION:

1. Open 3 terminals and run the following commands to start the nodes in the /node dirrectory. it is important to run these quite fast after eachother so they do not start to try to connect before all are up and running, furthermore it is important to run Localhost;5001 last as this is the leader by default
  Terminal 2: go run node.go node 2 5002 5001 5003
  Terminal 3: go run node.go node 3 5003 5001 5002
  Terminal 1: go run node.go node 1 5001 5002 5003

2. Open a 4. Terminal and use it to read or write to the auction with the following command structures.
   For bidding: go run node.go client bid <node-address> <bidder> <amount>
   For showing result: go run node.go client result <node-address>
Examples:
   go run node.go client bid localhost:5001 Alice 100
   go run node.go client result localhost:5001