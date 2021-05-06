import sys

import ndjson

output_path = str(sys.argv[1])
input_path = str(sys.argv[2])

with open(input_path, "r") as f:
    file_contents = ndjson.load(f)
data = file_contents[0]["data"]
with open(output_path, "w") as f:
    ndjson.dump(data, f)
