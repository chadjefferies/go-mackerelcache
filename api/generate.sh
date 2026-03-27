go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
protoc -I=api/mackerelcachepb --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative --go_out=api/mackerelcachepb --go-grpc_out=api/mackerelcachepb api/mackerelcachepb/services.proto api/mackerelcachepb/messages.proto