import configparser
import http
import logging
import logging.config

from pathlib import Path

import backoff
import requests
import urllib3

from asnake.client import ASnakeClient
from decouple import config, Csv

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def setup_logging(config_file='logging.conf'):
    config_path = Path(config_file)
    if config_path.exists():
        config = configparser.ConfigParser()
        try:
            config.read(config_file)
            logging.config.fileConfig(config_file)
        except Exception as e:
            print(f"ERROR READING {config_file}: {e}")
            fallback_logging()
    else:
        fallback_logging()

def fallback_logging():
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    print("LOGGING CONFIGURED USING basicConfig FALLBACK")

def execute(arg1, arg2):
    print("hello from cascflow execute()!")
    print(f"arg1: {arg1}, arg2: {arg2}")

asnake_client = None
def establish_archivesspace_connection():
    global asnake_client
    asnake_client = ASnakeClient(
        baseurl=config("ARCHIVESSPACE_API_URL"),
        username=config("ARCHIVESSPACE_USERNAME"),
        password=config("ARCHIVESSPACE_PASSWORD"),
    )
    logger.debug("🐞 ESTABLISHING A CONNECTION TO ARCHIVESSPACE")
    asnake_client.authorize()
    logger.debug(f'🐞 CONNECTION TO ARCHIVESSPACE ESTABLISHED: {config("ARCHIVESSPACE_API_URL")}')
    return

@backoff.on_exception(
    backoff.expo,
    (
        http.client.RemoteDisconnected,
        urllib3.exceptions.ProtocolError,
        urllib3.exceptions.NewConnectionError,
        urllib3.exceptions.MaxRetryError,
        requests.exceptions.ConnectionError,
    ),
    max_time=1800,
)
def archivessnake_get(uri):
    return asnake_client.get(uri)

def find_archival_object(component_id):
    """Returns a dict of the archival object data for a given component_id.

    Raises a ValueError if no archival object is found or if multiple archival
    objects are found.
    """
    find_uri = (
        f"/repositories/2/find_by_id/archival_objects?component_id[]={component_id}"
    )
    find_by_id_response = archivessnake_get(find_uri)
    if len(find_by_id_response.json()["archival_objects"]) < 1:
        raise ValueError(f"❌ ARCHIVAL OBJECT NOT FOUND: {component_id}")
    elif len(find_by_id_response.json()["archival_objects"]) > 1:
        raise ValueError(f"❌ MULTIPLE ARCHIVAL OBJECTS FOUND: {component_id}")
    else:
        archival_object = archivessnake_get(
            find_by_id_response.json()["archival_objects"][0]["ref"]
            + "?resolve[]=ancestors"
            + "&resolve[]=digital_object"
            + "&resolve[]=linked_agents"
            + "&resolve[]=repository"
            + "&resolve[]=subjects"
            + "&resolve[]=top_container"
        ).json()
        logger.info(f"☑️ ARCHIVAL OBJECT FOUND: {component_id}")
        return archival_object

def validate_source_path(source_volume):
    source_path = Path(config("COMMON_MOUNT_PARENT_PATH")).joinpath(source_volume, config("COMMON_SOURCE_PATH"))
    if not source_path.resolve().exists():
        raise FileNotFoundError(f"❌ SOURCE PATH '{source_path}' DOES NOT EXIST.")
    return source_path

def delete_files_to_remove(parent_path: Path):
    """Delete any files in the parent path that are listed in FILES_TO_REMOVE."""
    for f in parent_path.glob("**/*"):
        if f.is_file() and f.name in config("FILES_TO_REMOVE", default=None, cast=Csv()):
            f.unlink()
    return

def inspect_entry_directory(entry: Path, nested_directories: list, empty_directories: list) -> tuple:
    if any(child.is_dir() for child in entry.iterdir()):
        nested_directories.append(entry)
    if not any(child.is_file() for child in entry.iterdir()):
        empty_directories.append(entry)
    return nested_directories, empty_directories

def validate(source_volume: str) -> tuple:
    logger.debug(f"🐞 SOURCE_VOLUME: {source_volume}")

    if (source_path := Path(validate_source_path(source_volume))):
        logger.info(f"☑️ VALID SOURCE_VOLUME: {source_volume}")

    establish_archivesspace_connection()

    valid_archival_objects = []
    invalid_archival_objects = []
    nested_directories = []
    empty_directories = []
    file_count = 0  ## TBD store a list of files instead of only a count?
    logger.debug(f"🐞 SOURCE_PATH: {source_path}")
    delete_files_to_remove(source_path)
    ## iterate over the first level of entries in the source directory
    for entry in source_path.iterdir():
        ## validate the entry (file or directory)
        if find_archival_object(entry.stem):
            valid_archival_objects.append(entry.stem)
        else:
            invalid_archival_objects.append(entry.stem)
        ## count files in the root directory
        if entry.is_file():
            file_count += 1
        ## ensure directories do not contain subdirectories
        if entry.is_dir():
            inspection_nested, inspection_empty = inspect_entry_directory(entry, nested_directories, empty_directories)
            nested_directories.extend(inspection_nested)
            empty_directories.extend(inspection_empty)
            ## count files in the child directory
            for child in entry.iterdir():
                if child.is_file():
                    file_count += 1
    return {
        "source_path": source_path,
        "valid_archival_objects": valid_archival_objects,
        "invalid_archival_objects": invalid_archival_objects,
        "nested_directories": nested_directories,
        "empty_directories": empty_directories,
        "file_count": file_count,
    }
