#!/bin/bash
D=`date +"%Y-%m-%d-%H"`
cd /home/borescope/tools/syn_dns_ether
F=syn_dns_ether_600s_${D}-10.pcap
F2=out_syn_dns_ether_600s_${D}-10.txt
chown borescope.borescope ${F}
sudo tcpdump -n -i bond1 -tttt -vvv -r ${F} tcp | python3 filter.py ${F}
chown borescope.borescope ${F2}
#bzip2  ${F2}

