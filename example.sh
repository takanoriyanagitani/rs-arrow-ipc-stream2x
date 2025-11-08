#!/bin/sh

echo converting the stream to excel...
dirents2arrow-ipc-stream . |
	./arrow-ipc-stream2x \
		--sheet Sheet1 \
		--output out.xlsx

echo
echo printing the xlsx contents...
x2jsonl \
	--input ./out.xlsx \
	--sheet Sheet1 |
	jq -c
