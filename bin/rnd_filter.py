#!/usr/bin/env python

# filter that lets only a given proprtion of messages
# through at random

import random
import sys

random.seed()
for line in sys.stdin :
    if random.uniform(0.0,1.0) < float(sys.argv[1]) :
        print(line[:-1])
