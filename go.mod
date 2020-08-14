module github.com/protolambda/rumorhub

go 1.14

require (
	github.com/gorilla/websocket v1.4.2
	github.com/protolambda/rumor v0.2.3
)

replace github.com/protolambda/rumor v0.2.3 => ../rumor
