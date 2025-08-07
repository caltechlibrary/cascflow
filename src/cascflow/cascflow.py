import configparser
import http
import logging
import logging.config

from pathlib import Path

import backoff
import boto3
import requests
import urllib3

from asnake.client import ASnakeClient  # pypi: ArchivesSnake
from decouple import Csv  # pypi: python-decouple

# global config instance - can be overridden by consuming applications
_config = None


def get_config():
    """Get the config instance, importing default if none set."""
    global _config
    if _config is None:
        from decouple import config

        _config = config
    return _config


def set_config(config_instance):
    """Set a custom config instance for the library to use."""
    global _config
    _config = config_instance


# convenience function for backward compatibility
def config(*args, **kwargs):
    """Access configuration values through the current config instance."""
    return get_config()(*args, **kwargs)


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def setup_logging(config_file="logging.conf"):
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
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
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
                    new_file_uri_values[existing_file_version["file_uri"]] = (
                        existing_file_version
                    )
            # discard keys and create list of unique file_version dictionaries
            file_versions = list(new_file_uri_values.values())
            digital_object = instance["digital_object"]["_resolved"]
            digital_object["file_versions"] = file_versions
            digital_object["publish"] = True
            update_digital_object(digital_object["uri"], digital_object).json()


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
        logger.info(f"✳️ DIGITAL OBJECT CREATED: [{digital_object['title']}]({str(config('ARCHIVESSPACE_STAFF_URL')).rstrip('/')}/resolve/readonly?uri={digital_object_uri})")

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
    logger.debug(
        f"☑️  ARCHIVAL OBJECT UPDATED: {archival_object_post_response.json()['uri']}"
    )

    # TODO investigate how to roll back adding digital object to archival object

    # find_archival_object() again to include digital object instance
    archival_object = find_archival_object(archival_object["component_id"])

    return digital_object_uri, archival_object


def initialize_batch_directory(source_volume, batch_set_id, pipeline):
    source_path = Path(config("ABSOLUTE_MOUNT_PARENT")).joinpath(
        source_volume, config("RELATIVE_SOURCE_DIRECTORY")
    )
    logger.debug(f"🐞 SOURCE_PATH: {source_path}")
    batch_directory = Path(config("ABSOLUTE_MOUNT_PARENT")).joinpath(
        source_volume, config("RELATIVE_BATCH_DIRECTORY"), f"{batch_set_id}--{pipeline}"
    )
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
    return path_obj.rename(
        batch_directory.joinpath("STAGE_2_WORKING").joinpath(path_obj.name)
    )


def move_to_stage_3(path_obj: Path, batch_directory: Path):
    """Move the path object to the STAGE_3_COMPLETE directory."""
    logger.debug(f"🐞 PATH_OBJ: {path_obj}")
    logger.debug(f"🐞 BATCH_DIRECTORY: {batch_directory}")
    return path_obj.rename(
        batch_directory.joinpath("STAGE_3_COMPLETE").joinpath(path_obj.name)
    )


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
        logger.debug("☑️  ARRANGEMENT LEVELS AGGREGATED")
        return arrangement
    except:
        logger.exception("‼️")
        raise


def execute(source_volume: str, batch_set_id: str, pipeline: str):
    batch_directory = initialize_batch_directory(source_volume, batch_set_id, pipeline)
    ## delete any FILES_TO_REMOVE
    for f in batch_directory.glob("**/*"):
        if f.is_file() and f.name in config(
            "FILES_TO_REMOVE", default=None, cast=Csv()
        ):
            f.unlink()
    ## THE LOOP THAT HAS EVERYTHING IN IT
    for stage_1_path_obj in sorted(
        batch_directory.joinpath("STAGE_1_INITIAL").iterdir(), key=lambda obj: obj.name
    ):
        archival_object = find_archival_object(stage_1_path_obj.stem)
        logger.info(f"☑️ ARCHIVAL OBJECT: [{archival_object['id']}]({str(config('ARCHIVESSPACE_STAFF_URL')).rstrip('/')}/resolve/readonly?uri={archival_object['uri']})")
        arrangement = get_arrangement(archival_object)
        stage_2_path_obj = move_to_stage_2(stage_1_path_obj, batch_directory)
        if stage_2_path_obj.is_file():
            filepaths = [stage_2_path_obj]
        elif stage_2_path_obj.is_dir():
            filepaths = [i for i in stage_2_path_obj.iterdir() if i.is_file()]
        else:
            filepaths = []
        yield batch_directory, stage_2_path_obj, filepaths, archival_object, arrangement


asnake_client = None


def ensure_archivesspace_connection(func):
    """Decorator to ensure archivesspace connection is established before function call."""

    def wrapper(*args, **kwargs):
        global asnake_client
        if asnake_client is None:
            establish_archivesspace_connection()
        return func(*args, **kwargs)

    return wrapper


def establish_archivesspace_connection():
    global asnake_client
    # create client with standard parameters
    asnake_client = ASnakeClient(
        baseurl=config("ARCHIVESSPACE_API_URL"),
        username=config("ARCHIVESSPACE_USERNAME"),
        password=config("ARCHIVESSPACE_PASSWORD"),
    )
    # check for optional basic auth credentials
    basic_auth_username = config("ARCHIVESSPACE_BASIC_AUTH_USERNAME", default=None)
    basic_auth_password = config("ARCHIVESSPACE_BASIC_AUTH_PASSWORD", default=None)
    # add basic auth to the session if credentials are provided
    if basic_auth_username and basic_auth_password:
        # set basic auth on the session
        asnake_client.session.auth = (basic_auth_username, basic_auth_password)
    logger.debug("🐞 ESTABLISHING A CONNECTION TO ARCHIVESSPACE")
    try:
        asnake_client.authorize()
        logger.debug(
            f"🐞 CONNECTION TO ARCHIVESSPACE ESTABLISHED: {config('ARCHIVESSPACE_API_URL')}"
        )
    except Exception as e:
        logger.error(f"❌ FAILED TO ESTABLISH ARCHIVESSPACE CONNECTION: {e}")
        raise
    return


s3_client = None


def ensure_s3_connection(func):
    """Decorator to ensure S3 connection is established before function call."""

    def wrapper(*args, **kwargs):
        global s3_client
        if s3_client is None:
            establish_s3_connection()
        return func(*args, **kwargs)

    return wrapper


def establish_s3_connection():
    global s3_client
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=config("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=config("AWS_SECRET_ACCESS_KEY"),
    )
    logger.debug("🐞 CONNECTION TO S3 ESTABLISHED")
    return


@ensure_s3_connection
def s3_get_object(bucket, key):
    """Get an object from S3."""
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        return response
    except s3_client.exceptions.NoSuchKey:
        logger.error(f"❌ OBJECT NOT FOUND: {key} in {bucket}")
        return None
    except Exception as e:
        logger.error(f"❌ ERROR GETTING OBJECT: {e}")
        raise e


@ensure_s3_connection
def s3_put_object(bucket: str, key: str, body=b""):
    """Put an object to S3."""
    try:
        if not body:
            response = s3_client.put_object(
                Bucket=bucket,
                Key=key,
            )
        else:
            response = s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=body,
            )
        logger.debug(f"☑️ OBJECT PUT TO S3: {bucket}/{key}")
        return response
    except Exception as e:
        logger.error(f"❌ ERROR PUTTING OBJECT: {e}")
        raise e


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
@ensure_archivesspace_connection
def archivessnake_get(uri, params=None):
    if params:
        return asnake_client.get(uri, params=params)
    else:
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
@ensure_archivesspace_connection
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
        logger.debug(f"🐞 ARCHIVAL OBJECT FOUND: {component_id}")
        return archival_object


@ensure_s3_connection
def get_s3_resource_archival_object_paths(
    resource_id: str, bucket: str = None, path_prefix: str = None
):
    """
    Get a list of S3 paths for archival objects under a given resource prefix.

    Args:
        resource_id (str): The ArchivesSpace identifier for the resource.
        bucket (str, optional): S3 bucket name. If None, uses config("S3_BUCKET").
        path_prefix (str, optional): Prefix for S3 paths. If None, uses config("COMMON_PATH_PREFIX").

    Returns:
        list: A list of S3 paths for archival objects.
    """
    if bucket is None:
        bucket = config("S3_BUCKET")
    if path_prefix is None:
        path_prefix = config("COMMON_PATH_PREFIX")

    paginator = s3_client.get_paginator("list_objects_v2")
    archival_object_prefixes = []
    for result in paginator.paginate(
        Bucket=bucket,
        Delimiter="/",
        Prefix=f"{path_prefix}/{resource_id}/",
    ):
        for prefix in result.get("CommonPrefixes"):
            # store collection_id/component_id/
            archival_object_prefixes.append(prefix.get("Prefix"))
    return archival_object_prefixes


def validate_metadata_identifier(
    identifier: str,
    target: str = "",
    repository_id: str = "",
    bucket: str = "",
    path_prefix: str = "",
):
    """
    Validates a metadata identifier to determine if it represents a resource
    or archival object, and classifies associated archival objects by their existence
    in ArchivesSpace.

    The function operates in two modes based on the `target` parameter:

    **Metadata mode (target="metadata"):**
    - First checks if identifier matches a resource (`id_0`) in ArchivesSpace
    - If it's a resource: retrieves all published archival objects from S3 and validates each
    - If not a resource: treats identifier as an archival object and validates it directly

    **Other modes (target != "metadata"):**
    - Treats identifier as an archival object and validates it directly
    - Sets identifier_level to "archival_object"

    Validation is performed by attempting to find each archival object in ArchivesSpace.
    Objects that exist are classified as "eligible", those that don't are "ineligible".

    Args:
        identifier (str): The identifier to parse - either a resource ID or archival object component ID.
        target (str, optional): The workflow target. If "metadata", enables resource lookup.
            Defaults to "".
        repository_id (str, optional): ArchivesSpace repository ID. If empty, uses
            config("ARCHIVESSPACE_REPOSITORY_ID", default="2").
        bucket (str, optional): S3 bucket name for resource lookup. If empty, uses
            config("S3_BUCKET").
        path_prefix (str, optional): S3 path prefix for resource lookup. If empty, uses
            config("COMMON_PATH_PREFIX").

    Returns:
        dict: A dictionary containing:
            - "identifier_level" (str): Either "resource" or "archival_object".
            - "eligible_archival_objects" (dict): Component IDs mapped to full archival object data
              for objects that exist in ArchivesSpace.
            - "ineligible_archival_objects" (list[str]): Component IDs of archival objects
              that do not exist in ArchivesSpace.

    Note:
        - Uses Caltech Archives policy: only `id_0` field is used for resource identifiers
        - For resources, only published archival objects (those present in S3) are checked
        - Validation failures (archival objects not found) are caught and handled gracefully

    Example:
        >>> validate_metadata_identifier("CollectionID", target="metadata")
        {
            "identifier_level": "resource",
            "eligible_archival_objects": {"ComponentID_1": {...archival_object_data...}, "ComponentID_2": {...}},
            "ineligible_archival_objects": ["ComponentID_3"]
        }

        >>> validate_metadata_identifier("ComponentID_1", target="files")
        {
            "identifier_level": "archival_object",
            "eligible_archival_objects": {"ComponentID_1": {...archival_object_data...}},
            "ineligible_archival_objects": []
        }
    """
    if repository_id == "":
        repository_id = str(config("ARCHIVESSPACE_REPOSITORY_ID", default="2"))

    eligible_archival_objects = {}
    ineligible_archival_objects = []

    def classify_archival_object_eligibility(component_id: str) -> dict:
        try:
            archival_object = find_archival_object(component_id)
            eligible_archival_objects[component_id] = archival_object
        except ValueError:
            ineligible_archival_objects.append(component_id)
        return {
            "eligible_archival_objects": eligible_archival_objects,
            "ineligible_archival_objects": ineligible_archival_objects,
        }

    if target == "metadata":
        # the Update Metadata workflow accepts identifiers for either resources or archival objects
        find_resources_identifier_response = archivessnake_get(
            f'/repositories/{repository_id}/find_by_id/resources?identifier[]=["{identifier}"]',
        )
        find_archival_object_component_id_response = archivessnake_get(
            f"/repositories/{repository_id}/find_by_id/archival_objects?component_id[]={identifier}"
        )
        if (
            len(find_resources_identifier_response.json()["resources"]) == 1
            and len(find_archival_object_component_id_response.json()["archival_objects"])
            < 1
        ):
            # 👋 WE HAVE A RESOURCE
            identifier_level = "resource"
            # get the *PUBLISHED* archival objects under this resource from S3 (anything in S3 is published)
            component_identifiers = [
                p.split("/")[-2]
                for p in get_s3_resource_archival_object_paths(
                    identifier, bucket, path_prefix
                )
            ]
            for component_id in component_identifiers:
                classify_archival_object_eligibility(component_id)
        else:
            # 👋 WE HAVE AN ARCHIVAL OBJECT
            identifier_level = "archival_object"
            classify_archival_object_eligibility(identifier)
    else:
        # 👋 WE HAVE AN ARCHIVAL OBJECT
        # the Update Publication and Update Files workflows only accept identifiers for archival objects
        identifier_level = "archival_object"
        classify_archival_object_eligibility(identifier)
    return {
        "identifier_level": identifier_level,
        "eligible_archival_objects": eligible_archival_objects,
        "ineligible_archival_objects": ineligible_archival_objects,
    }


def validate_source_path(volume_name: str):
    source_path = Path(config("ABSOLUTE_MOUNT_PARENT")).joinpath(
        volume_name, config("RELATIVE_SOURCE_DIRECTORY")
    )
    if not source_path.resolve().exists():
        raise FileNotFoundError(f"❌ SOURCE PATH '{source_path}' DOES NOT EXIST.")
    return source_path


def delete_files_to_remove(parent_path: Path):
    """Delete any files in the parent path that are listed in FILES_TO_REMOVE."""
    for f in parent_path.glob("**/*"):
        if f.is_file() and f.name in config(
            "FILES_TO_REMOVE", default=None, cast=Csv()
        ):
            f.unlink()
    return


def inspect_entry_directory(
    entry: Path, nested_directories: list, empty_directories: list
) -> tuple:
    if any(child.is_dir() for child in entry.iterdir()):
        nested_directories.append(entry)
    if not any(child.is_file() for child in entry.iterdir()):
        empty_directories.append(entry)
    return nested_directories, empty_directories


def validate_digital_files(context: str) -> dict:
    """
    Validate the contents of a source directory and metadata for archival objects.

    This function performs validation based on the specified `target` type and `context`.
    It connects to the ArchivesSpace API to validate archival objects and inspects
    the directory structure and files in the source path for compliance with expected
    criteria.

    Args:
        context (str): The context for validation. For "metadata", this is the identifier
            for an archival object or resource. For "publication" or "files", this is the
            name of the source volume to validate.

    Returns:
        dict: A dictionary containing the results of the validation. Keys include:
            - "source_path" (Path): The resolved source path.
            - "eligible_archival_objects" (dict): Component IDs mapped to full archival object data
              for objects that passed validation.
            - "ineligible_archival_objects" (list): A list of archival object identifiers
              that failed validation.
            - "nested_directories" (list): A list of directories containing subdirectories.
            - "empty_directories" (list): A list of directories that are empty.
            - "file_count" (int): The total number of files in the source path and its
              subdirectories.

    Raises:
        FileNotFoundError: If the source path does not exist.
        ValueError: If an archival object is not found or multiple objects are found
            for a given identifier.
        Exception: For unexpected errors during validation.

    Notes:
        - For "metadata", the function currently uses `find_archival_object()` to validate
          archival objects and checks S3 bucket prefixes for resources.
        - For "publication" or "files", the function validates the directory structure,
          removes unwanted files listed in `FILES_TO_REMOVE`, and ensures directories
          do not contain subdirectories.

    Example:
        >>> validate("files", "source_volume_name")
        {
            "source_path": PosixPath("/path/to/source"),
            "eligible_archival_objects": {"obj1": {...archival_object_data...}, "obj2": {...}},
            "ineligible_archival_objects": ["obj3"],
            "nested_directories": [PosixPath("/path/to/source/nested_dir")],
            "empty_directories": [PosixPath("/path/to/source/empty_dir")],
            "file_count": 42,
        }
    """
    logger.debug(f"🐞 CONTEXT: {context}")

    source_path = validate_source_path(context)

    eligible_archival_objects = {}
    ineligible_archival_objects = []
    nested_directories = []
    empty_directories = []
    file_count = 0  ## TBD store a list of files instead of only a count?
    logger.debug(f"🐞 SOURCE_PATH: {source_path}")
    delete_files_to_remove(source_path)
    ## iterate over the first level of entries in the source directory
    for entry in source_path.iterdir():
        ## validate the entry (file or directory)
        validated_metadata_identifier = validate_metadata_identifier(entry.stem)
        if validated_metadata_identifier is not None:
            eligible_archival_objects.update(
                validated_metadata_identifier.get("eligible_archival_objects", {})
            )
            ineligible_archival_objects.extend(
                validated_metadata_identifier.get("ineligible_archival_objects", [])
            )
        ## count files in the root directory
        if entry.is_file():
            file_count += 1
        ## ensure directories do not contain subdirectories
        if entry.is_dir():
            inspection_nested, inspection_empty = inspect_entry_directory(
                entry, nested_directories, empty_directories
            )
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
