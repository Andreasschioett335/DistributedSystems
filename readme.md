How to Run the Application

1. Start the nodes

Open three terminals and run the following commands inside the /node directory.

Important notes:
- Start the nodes quickly one after another, so they do not attempt to connect before all are running.
- Run localhost:5001 last, since this node acts as the default leader.

Terminal 2:
go run node.go node 2 5002 5001 5003

Terminal 3:
go run node.go node 3 5003 5001 5002

Terminal 1 (Leader):
go run node.go node 1 5001 5002 5003

2. Interact with the auction

Open a fourth terminal to run client commands.

Place a bid:
go run node.go client bid <node-address> <bidder> <amount>

Show auction result:
go run node.go client result <node-address>

Examples:
go run node.go client bid localhost:5001 Alice 100
go run node.go client result localhost:5001
