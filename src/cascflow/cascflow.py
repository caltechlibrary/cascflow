import configparser
import http
import logging
import logging.config

from pathlib import Path

import backoff
import requests
import urllib3

from asnake.client import ASnakeClient  # pypi: ArchivesSnake
from decouple import config, Csv  # pypi: python-decouple

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

def update_digital_object(uri, data):
    # raises an HTTPError exception if unsuccessful
    response = archivessnake_post(uri, data)
    logger.debug(f"🐞 RESPONSE: {response.json()}")
    response.raise_for_status()
    return response


def save_digital_object_file_versions(archival_object, new_file_versions):
    for instance in archival_object["instances"]:
        if "digital_object" in instance.keys():
            # ASSUMPTION: only one digital_object exists per archival_object
            # TODO handle multiple digital_objects per archival_object
            existing_file_versions = instance["digital_object"]["_resolved"].get(
                "file_versions"
            )
            # create temporary dictionary of new_file_version values keyed by file_uri
            new_file_uri_values = {
                new_file_version["file_uri"]: new_file_version
                for new_file_version in new_file_versions
            }
            # add existing_file_version values to new_file_uri_values if file_uri not already present
            for existing_file_version in existing_file_versions:
                if existing_file_version["file_uri"] not in new_file_uri_values:
                    existing_file_version["publish"] = False
                    existing_file_version["is_representative"] = False
                    new_file_uri_values[
                        existing_file_version["file_uri"]
                    ] = existing_file_version
            # discard keys and create list of unique file_version dictionaries
            file_versions = list(new_file_uri_values.values())
            digital_object = instance["digital_object"]["_resolved"]
            digital_object["file_versions"] = file_versions
            digital_object["publish"] = True
            digital_object_post_response = update_digital_object(
                digital_object["uri"], digital_object
            ).json()


def create_digital_object(archival_object, digital_object_type=""):
    digital_object = {}
    digital_object["digital_object_id"] = archival_object["component_id"]  # required
    digital_object["title"] = archival_object["title"]  # required
    if digital_object_type:
        digital_object["digital_object_type"] = digital_object_type
    # NOTE leaving created digital objects unpublished
    # digital_object['publish'] = True

    digital_object_post_response = archivessnake_post(
        "/repositories/2/digital_objects", digital_object
    )
    # example success response:
    # {
    #     "id": 9189,
    #     "lock_version": 0,
    #     "stale": true,
    #     "status": "Created",
    #     "uri": "/repositories/2/digital_objects/9189",
    #     "warnings": []
    # }
    # example error response:
    # {
    #     "error": {
    #         "digital_object_id": [
    #             "Must be unique"
    #         ]
    #     }
    # }
    # TODO check for existing digital_object_id in validate()
    if "error" in digital_object_post_response.json():
        if "digital_object_id" in digital_object_post_response.json()["error"]:
            if (
                "Must be unique"
                in digital_object_post_response.json()["error"]["digital_object_id"]
            ):
                raise ValueError(
                    f"❌ NON-UNIQUE DIGITAL_OBJECT_ID: {archival_object['component_id']}"
                )
        else:
            raise RuntimeError(
                f"❌ UNEXPECTED ERROR: {digital_object_post_response.json()}"
            )
    else:
        digital_object_uri = digital_object_post_response.json()["uri"]
        logger.info(f"✳️  DIGITAL OBJECT CREATED: {digital_object_uri}")

    # set up a digital object instance to add to the archival object
    digital_object_instance = {
        "instance_type": "digital_object",
        "digital_object": {"ref": digital_object_uri},
    }
    # add digital object instance to archival object
    archival_object["instances"].append(digital_object_instance)
    # post updated archival object
    archival_object_post_response = archivessnake_post(
        archival_object["uri"], archival_object
    )
    logger.info(
        f'☑️  ARCHIVAL OBJECT UPDATED: {archival_object_post_response.json()["uri"]}'
    )

    # TODO investigate how to roll back adding digital object to archival object

    # find_archival_object() again to include digital object instance
    archival_object = find_archival_object(archival_object["component_id"])

    return digital_object_uri, archival_object


def initialize_batch_directory(source_volume, batch_set_id, pipeline):
    source_path = Path(config("ABSOLUTE_MOUNT_PARENT")).joinpath(source_volume, config("RELATIVE_SOURCE_DIRECTORY"))
    logger.debug(f"🐞 SOURCE_PATH: {source_path}")
    batch_directory = Path(config("ABSOLUTE_MOUNT_PARENT")).joinpath(source_volume, config("RELATIVE_BATCH_DIRECTORY"), f"{batch_set_id}--{pipeline}")
    logger.debug(f"🐞 BATCH_DIRECTORY: {batch_directory}")
    batch_directory.mkdir(parents=True, exist_ok=True)
    Path(source_path).rename(batch_directory.joinpath("STAGE_1_INITIAL"))
    batch_directory.joinpath("STAGE_2_WORKING").mkdir(parents=True, exist_ok=True)
    batch_directory.joinpath("STAGE_3_COMPLETE").mkdir(parents=True, exist_ok=True)
    Path(source_path).mkdir()
    return batch_directory

def move_to_stage_2(path_obj: Path, batch_directory: Path):
    """Move the path object to the STAGE_2_WORKING directory."""
    logger.debug(f"🐞 PATH_OBJ: {path_obj}")
    logger.debug(f"🐞 BATCH_DIRECTORY: {batch_directory}")
    return path_obj.rename(batch_directory.joinpath("STAGE_2_WORKING").joinpath(path_obj.name))


def move_to_stage_3(path_obj: Path, batch_directory: Path):
    """Move the path object to the STAGE_3_COMPLETE directory."""
    logger.debug(f"🐞 PATH_OBJ: {path_obj}")
    logger.debug(f"🐞 BATCH_DIRECTORY: {batch_directory}")
    return path_obj.rename(batch_directory.joinpath("STAGE_3_COMPLETE").joinpath(path_obj.name))


def get_arrangement(archival_object):
    """Return a dictionary of the arragement levels for an archival object.

    EXAMPLES:
    arrangement["repository_name"]
    arrangement["repository_code"]
    arrangement["archival_object_display_string"]
    arrangement["archival_object_level"]
    arrangement["archival_object_title"]
    arrangement["collection_title"]
    arrangement["collection_id"]
    arrangement["collection_uri"]
    arrangement["series_display_string"]
    arrangement["series_id"]
    arrangement["series_title"]
    arrangement["series_uri"]
    arrangement["subseries_display_string"]
    arrangement["subseries_id"]
    arrangement["subseries_title"]
    arrangement["subseries_uri"]
    arrangement["file_display_string"]
    arrangement["file_id"]
    arrangement["file_title"]
    arrangement["file_uri"]
    """
    try:
        # TODO document assumptions about arrangement
        arrangement = {}
        arrangement["repository_name"] = archival_object["repository"]["_resolved"][
            "name"
        ]
        arrangement["repository_code"] = archival_object["repository"]["_resolved"][
            "repo_code"
        ]
        arrangement["archival_object_display_string"] = archival_object[
            "display_string"
        ]
        arrangement["archival_object_level"] = archival_object["level"]
        arrangement["archival_object_title"] = archival_object.get("title")
        for ancestor in archival_object["ancestors"]:
            if ancestor["level"] == "collection":
                arrangement["collection_title"] = ancestor["_resolved"]["title"]
                arrangement["collection_id"] = ancestor["_resolved"]["id_0"]
                arrangement["collection_uri"] = ancestor["ref"]
            elif ancestor["level"] == "series":
                arrangement["series_display_string"] = ancestor["_resolved"][
                    "display_string"
                ]
                arrangement["series_id"] = ancestor["_resolved"].get("component_id")
                arrangement["series_title"] = ancestor["_resolved"].get("title")
                arrangement["series_uri"] = ancestor["ref"]
            elif ancestor["level"] == "subseries":
                arrangement["subseries_display_string"] = ancestor["_resolved"][
                    "display_string"
                ]
                arrangement["subseries_id"] = ancestor["_resolved"].get("component_id")
                arrangement["subseries_title"] = ancestor["_resolved"].get("title")
                arrangement["subseries_uri"] = ancestor["ref"]
            elif ancestor["level"] == "file":
                arrangement["file_display_string"] = ancestor["_resolved"][
                    "display_string"
                ]
                arrangement["file_id"] = ancestor["_resolved"].get("component_id")
                arrangement["file_title"] = ancestor["_resolved"].get("title")
                arrangement["file_uri"] = ancestor["ref"]
        logger.info("☑️  ARRANGEMENT LEVELS AGGREGATED")
        return arrangement
    except:
        logger.exception("‼️")
        raise


def execute(source_volume: str, batch_set_id: str, pipeline=None, **kwargs):
    establish_archivesspace_connection()
    batch_directory = initialize_batch_directory(source_volume, batch_set_id, pipeline)
    ## delete any FILES_TO_REMOVE
    for f in batch_directory.glob("**/*"):
        if f.is_file() and f.name in config("FILES_TO_REMOVE", default=None, cast=Csv()):
            f.unlink()
    ## THE LOOP THAT HAS EVERYTHING IN IT
    for stage_1_path_obj in sorted(batch_directory.joinpath("STAGE_1_INITIAL").iterdir(), key=lambda obj: obj.name):
        archival_object = find_archival_object(stage_1_path_obj.stem)
        arrangement = get_arrangement(archival_object)
        stage_2_path_obj = move_to_stage_2(stage_1_path_obj, batch_directory)
        if stage_2_path_obj.is_file():
            filepaths = [stage_2_path_obj]
        elif stage_2_path_obj.is_dir():
            filepaths = [i for i in stage_2_path_obj.iterdir() if i.is_file()]
        else:
            filepaths = []
        yield filepaths, archival_object, arrangement

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
def archivessnake_post(uri, object):
    return asnake_client.post(uri, json=object)


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
    source_path = Path(config("ABSOLUTE_MOUNT_PARENT")).joinpath(source_volume, config("RELATIVE_SOURCE_DIRECTORY"))
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

    eligible_archival_objects = []
    ineligible_archival_objects = []
    nested_directories = []
    empty_directories = []
    file_count = 0  ## TBD store a list of files instead of only a count?
    logger.debug(f"🐞 SOURCE_PATH: {source_path}")
    delete_files_to_remove(source_path)
    ## iterate over the first level of entries in the source directory
    for entry in source_path.iterdir():
        ## validate the entry (file or directory)
        if find_archival_object(entry.stem):
            eligible_archival_objects.append(entry.stem)
        else:
            ineligible_archival_objects.append(entry.stem)
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
        "eligible_archival_objects": eligible_archival_objects,
        "ineligible_archival_objects": ineligible_archival_objects,
        "nested_directories": nested_directories,
        "empty_directories": empty_directories,
        "file_count": file_count,
    }
