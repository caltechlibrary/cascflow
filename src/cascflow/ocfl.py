"""OCFL (Oxford Common File Layout) helpers.

Pure functions only -- no S3/network/filesystem I/O. See
https://ocfl.io/latest/spec/ and
https://ocfl.github.io/extensions/0007-n-tuple-omit-prefix-storage-layout.html
"""

import copy

OCFL_INVENTORY_TYPE = "https://ocfl.io/1.1/spec/#inventory"


def object_root_path(
    object_id: str,
    delimiter: str = ":",
    tuple_size: int = 3,
    number_of_tuples: int = 3,
    zero_padding: str = "left",
    reverse_object_root: bool = False,
) -> str:
    """Return the object root path for `object_id` per OCFL Extension 0007
    (N Tuple Omit Prefix Storage Layout).

    Defaults match the extension's own defaults. Caltech Archives' ARK-based
    configuration uses `delimiter="/"` and `number_of_tuples=2` -- see the
    distillery repo's OCFL-PRESERVATION-PLAN.md, §3.
    """
    if zero_padding not in ("left", "right"):
        raise ValueError(f"zero_padding must be 'left' or 'right': {zero_padding!r}")

    if delimiter:
        index = object_id.rfind(delimiter)
        if index == -1:
            remainder = object_id
        else:
            remainder = object_id[index + len(delimiter) :]
            if not remainder:
                raise ValueError(
                    f"delimiter {delimiter!r} occurs at the end of the "
                    f"identifier: {object_id!r}"
                )
    else:
        remainder = object_id

    target_length = tuple_size * number_of_tuples
    working = remainder
    if len(working) < target_length:
        padding = "0" * (target_length - len(working))
        working = padding + working if zero_padding == "left" else working + padding

    if reverse_object_root:
        working = working[::-1]

    tuples = [
        working[i * tuple_size : (i + 1) * tuple_size] for i in range(number_of_tuples)
    ]

    return "/".join([*tuples, remainder])


def build_inventory(object_id: str, digest_algorithm: str = "sha256") -> dict:
    """Return a new, version-less OCFL inventory shell for `object_id`."""
    return {
        "id": object_id,
        "type": OCFL_INVENTORY_TYPE,
        "digestAlgorithm": digest_algorithm,
        "head": None,
        "manifest": {},
        "versions": {},
    }


def add_version(
    inventory: dict,
    files: dict,
    created: str,
    message: str = "",
    user_name: str = "",
    user_address: str = "",
    version_padding: int = 4,
    fixity: dict | None = None,
) -> dict:
    """Return a new inventory with one additional version appended.

    `files` maps every logical path present as of this version to its
    digest (using `inventory["digestAlgorithm"]`) -- the complete state of
    the object, not just what changed. A digest already present in the
    manifest from an earlier version is reused rather than duplicated
    (OCFL's normal forward-delta behavior); a new digest gets a new content
    path under this version's content directory.

    `version_padding` sets the zero-padded width of version numbers (e.g. 4
    -> "v0001") and only matters when creating an object's first version --
    every later version infers its width from the existing head instead, so
    padding can never drift inconsistently across an object's lifetime (OCFL
    requires every version number in an object to share the same width).
    Pass `version_padding=0` for unpadded version numbers ("v1", "v2", ...).

    `fixity` optionally records secondary digests in the inventory's OCFL
    "fixity" block: `{algorithm: {logical_path: digest}}`, e.g.
    `{"md5": {"a.tif": "<md5 hex>"}}`. Purely supplementary -- OCFL itself
    only relies on `digestAlgorithm`/`manifest` for integrity checking.

    `message`, `user_name`, and `user_address` are all optional per the OCFL
    spec and are omitted from the version block entirely when not provided,
    rather than being written as empty strings. `user_address` is only
    meaningful (and only included) when `user_name` is also given.

    Does not mutate `inventory`.
    """
    inventory = copy.deepcopy(inventory)

    head = inventory["head"]
    if head is None:
        version_number = 1
        padding = version_padding
    else:
        digits = head[1:]
        version_number = int(digits) + 1
        padding = len(digits)
    version_id = f"v{version_number:0{padding}d}" if padding else f"v{version_number}"

    manifest = inventory["manifest"]
    state = {}
    content_path_by_logical_path = {}
    for logical_path, digest in files.items():
        state.setdefault(digest, []).append(logical_path)
        if digest not in manifest:
            manifest[digest] = [f"{version_id}/content/{logical_path}"]
        content_path_by_logical_path[logical_path] = manifest[digest][0]

    version = {"created": created, "state": state}
    if message:
        version["message"] = message
    if user_name:
        user = {"name": user_name}
        if user_address:
            user["address"] = user_address
        version["user"] = user
    inventory["versions"][version_id] = version
    inventory["head"] = version_id

    if fixity:
        inventory_fixity = inventory.setdefault("fixity", {})
        for algorithm, digests_by_logical_path in fixity.items():
            algorithm_fixity = inventory_fixity.setdefault(algorithm, {})
            for logical_path, digest in digests_by_logical_path.items():
                content_path = content_path_by_logical_path[logical_path]
                paths = algorithm_fixity.setdefault(digest, [])
                if content_path not in paths:
                    paths.append(content_path)

    return inventory
