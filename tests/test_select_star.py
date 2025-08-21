import json

import pytest

from py_dbt_cll.dbt_lineage import DbtCLL


@pytest.fixture
def ccl():
    with open("tests/manifest.json", "r", encoding="utf-8") as file:
        manifest_data = json.load(file)
    return DbtCLL(manifest_data)


def test_select_star(ccl):
    sql = """
        with
            cte as (select * from "master"."bds"."bds_gen__datespine")
        select *
        from cte
    """
    columns = ["date_day"]
    lineage = ccl.extract_cll(sql, columns, debug=False)
    assert lineage is not None
    assert "date_day" in lineage

    assert "bds.bds_gen__datespine.date_day" in lineage["date_day"]
