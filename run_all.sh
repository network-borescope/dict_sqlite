#!/bin/bash

for filename in syn_dns_ether_600s*.pcap; do
	echo "Processing $filename"
	sudo tcpdump -n -i bond1 -tttt -vvv -r "$filename" tcp | python3 filter.py "$filename"
done
