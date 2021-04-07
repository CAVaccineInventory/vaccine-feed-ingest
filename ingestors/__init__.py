import logging

# configure logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
    datefmt="%m/%d/%Y %H:%M:%S",
)

# configure user-agent
agent_string = "Vaccinebot (+https://vaccinateca.com/vaccinebot)"
header_dict = {"user-agent": agent_string}  # for urllib3
