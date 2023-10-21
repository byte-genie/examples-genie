import logging

# Configure logging to console with module name
logging.basicConfig(
    level=logging.DEBUG,  # Set the desired log level
    format='%(asctime)s [%(levelname)s] - %(name)s:%(filename)s:%(lineno)d - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S GMT'  # Define the date format if needed
)

# Create a logger for the current module
logger = logging.getLogger("examples-genie")

# Configure logging to console with module name
logging.basicConfig(
    level=logging.DEBUG,  # Set the desired log level
    format='%(asctime)s [%(levelname)s] - %(name)s:%(filename)s:%(lineno)d - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'  # Define the date format if needed
)

EXT_LIB = ['urllib', 'urllib3', 'asyncio', 'tzlocal', 'openai',
           'PIL.PngImagePlugin']
for lib in EXT_LIB:
    _logger = logging.getLogger(lib)
    _logger.setLevel(logging.WARNING)
