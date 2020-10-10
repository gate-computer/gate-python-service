PYTHON		?= python3
WORKSPACE	:= $(shell pwd)/..

.PHONY: generate
generate:
	$(PYTHON) -m grpc_tools.protoc -I "$(WORKSPACE)" --python_out=. --grpc_python_out=. "$(WORKSPACE)/gate/service/grpc/api/service.proto"
