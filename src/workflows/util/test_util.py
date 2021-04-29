import workflows.util


def test_generated_host_ids_are_somewhat_sensible():
    hostid = workflows.util.generate_unique_host_id()
    assert " " not in hostid
    assert "." in hostid
