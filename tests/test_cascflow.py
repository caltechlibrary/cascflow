from cascflow import cascflow as cascflow_module


class FakeResponse:
    def __init__(self, data):
        self._data = data

    def json(self):
        return self._data

    def raise_for_status(self):
        pass


def test_enrich_ancestors_resolves_each_ancestors_own_linked_agents_and_subjects(
    monkeypatch,
):
    archival_object = {
        "ancestors": [
            {"ref": "/repositories/2/resources/1", "level": "collection"},
            {"ref": "/repositories/2/archival_objects/2", "level": "series"},
        ]
    }
    responses = {
        "/repositories/2/resources/1": {
            "title": "Collection Title",
            "linked_agents": [{"_resolved": {"name": "Someone"}}],
        },
        "/repositories/2/archival_objects/2": {
            "title": "Series Title",
            "subjects": [{"_resolved": {"term": "Something"}}],
        },
    }

    def fake_archivesspace_get(uri, params=None):
        base_uri, _, query = uri.partition("?")
        assert query == "resolve[]=linked_agents&resolve[]=subjects"
        return FakeResponse(responses[base_uri])

    monkeypatch.setattr(cascflow_module, "archivesspace_get", fake_archivesspace_get)

    result = cascflow_module.enrich_ancestors(archival_object)

    assert result["ancestors"][0]["_resolved"]["title"] == "Collection Title"
    assert (
        result["ancestors"][0]["_resolved"]["linked_agents"][0]["_resolved"]["name"]
        == "Someone"
    )
    assert result["ancestors"][1]["_resolved"]["title"] == "Series Title"
    assert (
        result["ancestors"][1]["_resolved"]["subjects"][0]["_resolved"]["term"]
        == "Something"
    )


def test_enrich_ancestors_does_not_mutate_the_input(monkeypatch):
    archival_object = {"ancestors": [{"ref": "/repositories/2/resources/1"}]}
    monkeypatch.setattr(
        cascflow_module,
        "archivesspace_get",
        lambda uri, params=None: FakeResponse({"title": "Collection Title"}),
    )

    cascflow_module.enrich_ancestors(archival_object)

    assert "_resolved" not in archival_object["ancestors"][0]


def test_enrich_ancestors_handles_no_ancestors_key():
    archival_object = {"component_id": "aspace_1"}

    result = cascflow_module.enrich_ancestors(archival_object)

    assert result == {"component_id": "aspace_1"}


def test_save_digital_object_file_versions_refetches_instead_of_trusting_resolved(
    monkeypatch,
):
    # archival_object's embedded digital_object["_resolved"] snapshot can go
    # stale by the time this runs (e.g. an intervening archival_object save
    # can bump the linked digital_object's lock_version), which 409s the
    # update if we trust it instead of re-fetching. Deliberately no
    # "_resolved" key here at all, to prove it's not read.
    archival_object = {
        "title": "A Title",
        "instances": [
            {
                "instance_type": "digital_object",
                "digital_object": {"ref": "/repositories/2/digital_objects/1"},
            }
        ],
    }
    current_digital_object = {
        "uri": "/repositories/2/digital_objects/1",
        "lock_version": 5,
        "file_versions": [{"file_uri": "http://example.com/existing.tif"}],
    }
    get_calls = []
    post_calls = []

    def fake_archivesspace_get(uri, params=None):
        get_calls.append(uri)
        return FakeResponse(current_digital_object)

    def fake_archivesspace_post(uri, obj):
        post_calls.append((uri, obj))
        return FakeResponse(obj)

    monkeypatch.setattr(cascflow_module, "archivesspace_get", fake_archivesspace_get)
    monkeypatch.setattr(cascflow_module, "archivesspace_post", fake_archivesspace_post)

    cascflow_module.save_digital_object_file_versions(
        archival_object, [{"file_uri": "http://example.com/new.tif"}]
    )

    assert get_calls == ["/repositories/2/digital_objects/1"]
    assert len(post_calls) == 1
    posted_uri, posted_digital_object = post_calls[0]
    assert posted_uri == "/repositories/2/digital_objects/1"
    assert posted_digital_object["lock_version"] == 5
    assert posted_digital_object["title"] == "A Title"
    assert {fv["file_uri"] for fv in posted_digital_object["file_versions"]} == {
        "http://example.com/new.tif",
        "http://example.com/existing.tif",
    }
