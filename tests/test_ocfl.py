from cascflow import ocfl


def test_object_root_path_matches_caltech_ark_configuration_no_padding_needed():
    # delimiter="/" (not the extension default ":") is required for ARK URLs:
    # "ark:99999" itself contains a colon, so ":" would strip too little.
    path = ocfl.object_root_path(
        "https://n2t.net/ark:99999/b3xq7z",
        delimiter="/",
        tuple_size=3,
        number_of_tuples=2,
    )

    assert path == "b3x/q7z/b3xq7z"


def test_object_root_path_pads_short_remainder_on_the_left():
    path = ocfl.object_root_path(
        "urn:foo:ab",
        delimiter=":",
        tuple_size=3,
        number_of_tuples=2,
        zero_padding="left",
    )

    assert path == "000/0ab/ab"


def test_object_root_path_pads_short_remainder_on_the_right():
    path = ocfl.object_root_path(
        "urn:foo:ab",
        delimiter=":",
        tuple_size=3,
        number_of_tuples=2,
        zero_padding="right",
    )

    assert path == "ab0/000/ab"


def test_object_root_path_reverses_when_configured():
    path = ocfl.object_root_path(
        "x:abcdef",
        delimiter=":",
        tuple_size=3,
        number_of_tuples=2,
        reverse_object_root=True,
    )

    # tuples come from the reversed/padded string; the final segment is
    # always the real (unreversed, unpadded) remainder.
    assert path == "fed/cba/abcdef"


def test_object_root_path_uses_whole_identifier_when_delimiter_absent():
    path = ocfl.object_root_path(
        "foobar",
        delimiter=":",
        tuple_size=3,
        number_of_tuples=2,
    )

    assert path == "foo/bar/foobar"


def test_object_root_path_raises_when_delimiter_is_at_the_end():
    try:
        ocfl.object_root_path("foo:", delimiter=":")
    except ValueError:
        pass
    else:
        raise AssertionError("expected a ValueError")


def test_object_root_path_rejects_invalid_zero_padding():
    try:
        ocfl.object_root_path("foo:bar", zero_padding="sideways")
    except ValueError:
        pass
    else:
        raise AssertionError("expected a ValueError")


def test_object_root_path_matches_extension_defaults():
    # sanity check against the extension's own documented defaults
    # (delimiter=":", tuple_size=3, number_of_tuples=3).
    path = ocfl.object_root_path("namespace:abc")

    assert path == "000/000/abc/abc"


def test_build_inventory_returns_versionless_shell_with_supplied_id():
    inventory = ocfl.build_inventory("https://n2t.net/ark:99999/b3xq7z")

    assert inventory == {
        "id": "https://n2t.net/ark:99999/b3xq7z",
        "type": ocfl.OCFL_INVENTORY_TYPE,
        "digestAlgorithm": "sha256",
        "head": None,
        "manifest": {},
        "versions": {},
    }


def test_build_inventory_accepts_a_different_digest_algorithm():
    inventory = ocfl.build_inventory("id1", digest_algorithm="sha512")

    assert inventory["digestAlgorithm"] == "sha512"


def test_add_version_creates_v1_with_expected_state_and_manifest():
    inventory = ocfl.build_inventory("id1")

    result = ocfl.add_version(
        inventory,
        files={"archival_object.json": "digestA", "photo.tif": "digestB"},
        created="2026-01-01T00:00:00Z",
        message="initial version",
        user_name="distillery",
        user_address="mailto:archives@caltech.edu",
    )

    # version numbers are zero-padded to 4 digits by default (see
    # test_add_version_supports_unpadded_version_numbers for the override).
    assert result["head"] == "v0001"
    assert result["versions"]["v0001"] == {
        "created": "2026-01-01T00:00:00Z",
        "state": {
            "digestA": ["archival_object.json"],
            "digestB": ["photo.tif"],
        },
        "message": "initial version",
        "user": {"name": "distillery", "address": "mailto:archives@caltech.edu"},
    }
    assert result["manifest"] == {
        "digestA": ["v0001/content/archival_object.json"],
        "digestB": ["v0001/content/photo.tif"],
    }


def test_add_version_does_not_mutate_the_input_inventory():
    inventory = ocfl.build_inventory("id1")

    result = ocfl.add_version(inventory, {"a.txt": "d1"}, created="t1")

    assert inventory["head"] is None
    assert inventory["manifest"] == {}
    assert result is not inventory


def test_add_version_increments_head_and_reuses_unchanged_digests():
    v1 = ocfl.add_version(ocfl.build_inventory("id1"), {"a.txt": "d1"}, created="t1")

    v2 = ocfl.add_version(v1, {"a.txt": "d1", "b.txt": "d2"}, created="t2")

    assert v1["head"] == "v0001"
    assert v2["head"] == "v0002"
    assert v2["versions"]["v0001"] == v1["versions"]["v0001"]
    assert v2["versions"]["v0002"]["state"] == {"d1": ["a.txt"], "d2": ["b.txt"]}
    # d1's manifest entry stays at its original v0001 content path -- it
    # isn't re-copied into v0002 just because it's still present.
    assert v2["manifest"] == {
        "d1": ["v0001/content/a.txt"],
        "d2": ["v0002/content/b.txt"],
    }


def test_add_version_records_multiple_logical_paths_for_the_same_digest():
    result = ocfl.add_version(
        ocfl.build_inventory("id1"),
        files={"a.txt": "dup", "b.txt": "dup"},
        created="t1",
    )

    assert result["versions"]["v0001"]["state"]["dup"] == ["a.txt", "b.txt"]
    # only one content path is recorded for the shared digest
    assert result["manifest"]["dup"] == ["v0001/content/a.txt"]


def test_add_version_supports_unpadded_version_numbers():
    v1 = ocfl.add_version(
        ocfl.build_inventory("id1"), {"a.txt": "d1"}, created="t1", version_padding=0
    )

    v2 = ocfl.add_version(v1, {"a.txt": "d1", "b.txt": "d2"}, created="t2")

    assert v1["head"] == "v1"
    # v2 infers width 0 from v1's head, ignoring the version_padding=4
    # default -- padding can't drift once the first version sets it.
    assert v2["head"] == "v2"


def test_add_version_infers_padding_width_from_existing_head():
    v1 = ocfl.add_version(
        ocfl.build_inventory("id1"), {"a.txt": "d1"}, created="t1", version_padding=2
    )

    v2 = ocfl.add_version(v1, {"a.txt": "d1", "b.txt": "d2"}, created="t2")

    assert v1["head"] == "v01"
    assert v2["head"] == "v02"


def test_add_version_records_optional_fixity_block():
    result = ocfl.add_version(
        ocfl.build_inventory("id1"),
        files={"a.txt": "sha_a", "b.txt": "sha_b"},
        created="t1",
        fixity={"md5": {"a.txt": "md5_a", "b.txt": "md5_b"}},
    )

    assert result["fixity"] == {
        "md5": {
            "md5_a": ["v0001/content/a.txt"],
            "md5_b": ["v0001/content/b.txt"],
        }
    }


def test_add_version_omits_fixity_block_when_not_provided():
    result = ocfl.add_version(
        ocfl.build_inventory("id1"), {"a.txt": "sha_a"}, created="t1"
    )

    assert "fixity" not in result


def test_add_version_accumulates_fixity_across_versions():
    v1 = ocfl.add_version(
        ocfl.build_inventory("id1"),
        {"a.txt": "sha_a"},
        created="t1",
        fixity={"md5": {"a.txt": "md5_a"}},
    )

    v2 = ocfl.add_version(
        v1,
        {"a.txt": "sha_a", "b.txt": "sha_b"},
        created="t2",
        fixity={"md5": {"b.txt": "md5_b"}},
    )

    assert v2["fixity"] == {
        "md5": {
            "md5_a": ["v0001/content/a.txt"],
            "md5_b": ["v0002/content/b.txt"],
        }
    }
