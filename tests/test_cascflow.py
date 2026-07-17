from cascflow import cascflow as cascflow_module


class FakeResponse:
    def __init__(self, data):
        self._data = data

    def json(self):
        return self._data


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
