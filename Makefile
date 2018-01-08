.PHONY: python clean

compile: serializemessage_python 

clean:
	rm -f serializemessage_python
	rm -f protoc_m tweet_sms_pb2.py
	rm -f *.pyc

protoc_m: tweet_sms.proto
	protoc $$PROTO_PATH --python_out=. tweet_sms.proto
	@touch protoc_m

serializemessage_python: protoc_m
	@echo "Writing shortcut script serializemessage_python..."
	@echo '#! /bin/sh' > serializemessage_python
	@echo './serializemessage.py "$$@"' >> serializemessage_python
	@chmod +x serializemessage_python

run:
	python TwitterAPIStreaming.py