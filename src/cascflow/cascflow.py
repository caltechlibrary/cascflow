import http
import logging

from pathlib import Path

import backoff
import requests
import urllib3

from asnake.client import ASnakeClient
from decouple import config, Csv

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

import logging
import logging.config
from pathlib import Path
import configparser

def setup_logging(config_file='logging.conf'):
    config_path = Path(config_file)
    if config_path.exists():
        config = configparser.ConfigParser()
        try:
            config.read(config_file)
            logging.config.fileConfig(config_file)
            print(f"LOGGING CONFIGURED USING {config_file}")
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

call_is_publication_file_supported = None
def register_publication_supported_file_checker(callback):
    global call_is_publication_file_supported
    call_is_publication_file_supported = callback
    return callback

call_is_archival_object_valid_for_publication = None
def register_archival_object_valid_for_publication_checker(callback):
    global call_is_archival_object_valid_for_publication
    call_is_archival_object_valid_for_publication = callback
    return callback

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

def is_archival_object_valid(component_id, pipeline=None, **kwargs):
    try:
        archival_object = find_archival_object(component_id)
    except Exception as e:
        logger.exception(e)
    if pipeline == "publication":
        if call_is_archival_object_valid_for_publication:
            if call_is_archival_object_valid_for_publication(archival_object, **kwargs):
                logger.info(f"☑️ ARCHIVAL OBJECT VALID FOR PUBLICATION: {component_id}")
            else:
                message = "❌ ARCHIVAL OBJECT NOT VALID FOR PUBLICATION: [**{}**]({}/resolve/readonly?uri={})".format(
                    archival_object["title"],
                    config("ASPACE_STAFF_URL"),
                    archival_object["uri"],
                )
                logger.error(message)
                return False
        else:
            logger.info(f"☑️ ARCHIVAL OBJECT VALID FOR PUBLICATION: {component_id}")
    else:
        ## no need to check more conditions
        return True

def validate_source_path(source_volume):
    SOURCE_PATH = Path(config("COMMON_MOUNT_PARENT_PATH")).joinpath(source_volume, config("COMMON_SOURCE_PATH"))
    if not SOURCE_PATH.resolve().exists():
        raise FileNotFoundError(f"SOURCE PATH '{SOURCE_PATH}' DOES NOT EXIST.")
    return SOURCE_PATH

def validate(source_volume, pipeline=None, **kwargs):
    logger.debug(f"🐞 SOURCE_VOLUME: {source_volume}")

    if (SOURCE_PATH := Path(validate_source_path(source_volume))):
        logger.info(f"☑️ VALID SOURCE_VOLUME: {source_volume}")

    establish_archivesspace_connection()

    valid_archival_objects = []
    invalid_archival_objects = []
    nested_directories = []
    empty_directories = []
    file_count = 0  # TBD store a list of files instead of only a count?
    unsupported_files = []
    logger.debug(f"🐞 SOURCE_PATH: {SOURCE_PATH}")
    ## delete any FILES_TO_REMOVE
    for f in SOURCE_PATH.glob("**/*"):
        if f.is_file() and f.name in config("FILES_TO_REMOVE", default=None, cast=Csv()):
            f.unlink()
    ## iterate over the first level of entries in the source directory
    for entry in SOURCE_PATH.iterdir():
        ## validate the entry (file or directory)
        if is_archival_object_valid(entry.stem, pipeline, **kwargs):
            valid_archival_objects.append(entry.stem)
        else:
            invalid_archival_objects.append(entry.stem)
        ## count files in the root directory
        if entry.is_file():
            file_count += 1
            ## check for file types that are unsupported in publication
            if pipeline == 'publication' and call_is_publication_file_supported:
                if call_is_publication_file_supported(entry):
                    unsupported_files.append(entry)
        ## ensure directories do not contain subdirectories
        if entry.is_dir():
            if any(child.is_dir() for child in entry.iterdir()):
                nested_directories.append(entry)
            if not any(child.is_file() for child in entry.iterdir()):
                empty_directories.append(entry)
            ## count files in the child directory
            for child in entry.iterdir():
                if child.is_file():
                    file_count += 1
                    ## check for file types that are unsupported in publication
                    if pipeline == 'publication' and call_is_publication_file_supported:
                        if call_is_publication_file_supported(entry):
                            unsupported_files.append(entry)
    ## messaging
    if valid_archival_objects:
        for valid_archival_object in valid_archival_objects:
            logger.info(f"🉑 {valid_archival_object}")
        logger.info(f"🗂 ARCHIVAL OBJECT COUNT: {len(valid_archival_objects)}")
        logger.info(f"📄 FILE COUNT: {file_count}")
    ## validation and messaging
    if file_count == 0:
        message = "❌ NO FILES FOUND"
        logger.error(message)
        raise FileNotFoundError(message)
    if invalid_archival_objects:
        message = "❌ INVALID ARCHIVAL OBJECTS FOUND"
        logger.error(message)
        for invalid_archival_object in invalid_archival_objects:
            logger.error(f"❌ {invalid_archival_object}")
        raise RuntimeError(message)
    if nested_directories:
        message = "❌ NESTED DIRECTORIES FOUND"
        logger.error(message)
        for nested_directory in nested_directories:
            logger.error(f"❌ {nested_directory}")
        raise RuntimeError(message)
    if empty_directories:
        message = "❌ EMPTY DIRECTORIES FOUND"
        logger.error(message)
        for empty_directory in empty_directories:
            logger.error(f"❌ {empty_directory}")
        raise RuntimeError(message)
    if unsupported_files:
        message = "❌ UNSUPPORTED FILE TYPES FOUND"
        logger.error(message)
        for unsupported_file in unsupported_files:
            logger.error(f"❌ {unsupported_file}")
        raise RuntimeError(message)
