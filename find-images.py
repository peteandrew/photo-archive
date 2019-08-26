#!env python

import sys

from dynamo_access import find_images
from dateutil.parser import parse

if len(sys.argv) < 3:
    sys.stderr.write("Must supply from datetime and to datetime\n")
    sys.exit(1)

from_datetime = parse(sys.argv[1])
to_datetime = parse(sys.argv[2])

print(find_images(from_datetime, to_datetime))
