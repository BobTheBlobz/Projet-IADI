from pathlib import Path

# ----------------------------------------------
# CONSTANTS
# ----------------------------------------------

# The ElasticSearch host coordinates. In our case, it's the local machine.
ES_HOST = {"host" : "localhost", "port" : 9200}

# The index name for the raw xml data. Beware, changing it will rewrite all data on the disk!
DATA_INDEX = 'projet'

# The index name for the processed vectorized data. Beware, changing it will rewrite all data on the disk!
VECTORS_INDEX = 'vectors'

# Type names for indexing purpose
FLOW_TYPE_NAME = 'Flow'
VECTOR_TYPE_NAME = 'VectorizedFlow'

# ???
ID_FIELD = 'key'

# The maximum Bulk Size. Bulks are chunks of data that allows faster searches. No need to change it.
BULK_SIZE = 1000

# The maximum results displayed for a single research.
MAX_DISPLAY_SIZE = 10000

# The timeout in minutes after which the search sends out an exception.
TIMEOUT = '2m'

# The working directory of the program. The directory containing the XML files must be names 'XMLFiles'.
XMLdirectory = Path(Path().absolute(),Path("XMLFiles"))
SavesDirectory = Path(Path().absolute(),Path("Binaries"))
print("Directory Path:", Path().absolute())
print("XML Files Path:", XMLdirectory)

# DICTIONARIES: Generated when launching the program. Used for vectorisation.
PROTOCOL_DICT = { "EMPTY" : 0, "UNKNOWN" : -1 }
DIRECTION_DICT = { "EMPTY" : 0, "UNKNOWN" : -1 }
TAG_DICT = { "EMPTY" : 0, "UNKNOWN" : -1 }
FLAG_DICT = { "EMPTY" : 0, "UNKNOWN" : 1 , "F" : 2, "S" : 3, "R" : 4, "P" : 5, "A" : 6, "U" : 7 }
