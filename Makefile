PYTHON		?= python3
WORKSPACE	:= ..

.PHONY: generate
generate:
	$(PYTHON) -m grpc_tools.protoc -I "$(WORKSPACE)" --python_out=. --grpc_python_out=. "$(WORKSPACE)/gate/grpc/pb/service.proto"
