import logging

# Configure logging to console with module name
logging.basicConfig(
    level=logging.DEBUG,  # Set the desired log level
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Define the log format
    datefmt='%Y-%m-%d %H:%M:%S'  # Define the date format if needed
)

# Create a logger for the current module
logger = logging.getLogger("flask-genie")

# Configure logging to console with module name
logging.basicConfig(
    level=logging.INFO,  # Set the desired log level
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Define the log format
    datefmt='%Y-%m-%d %H:%M:%S'  # Define the date format if needed
)

EXT_LIB = ['urllib', 'urllib3', 'asyncio', 'tzlocal']
for lib in EXT_LIB:
    _logger = logging.getLogger(lib)
    _logger.setLevel(logging.WARNING)
