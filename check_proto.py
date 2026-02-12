import protocol
import sys

file_name = sys.argv[1]
p = protocol.get_pickle_protocol(file_name)
print("Pickle protocol:", p)