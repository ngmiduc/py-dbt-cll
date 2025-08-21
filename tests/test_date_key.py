import json

import pytest

from py_dbt_cll.dbt_lineage import DbtCLL


@pytest.fixture
def ccl():
    with open("tests/manifest_date_key.json", "r", encoding="utf-8") as file:
        manifest_data = json.load(file)
    return DbtCLL(manifest_data)


def test_lineage(ccl):
    sql = """
        with
            cte_source__datespine as (select * from "master"."bds"."bds_gen__datespine"),
            cte_source__term as (
                select * from "master"."stg_student_mgt"."stg_student_mgt__term"
            ),
            cte_joined as (
                select cte_source__datespine.date_key + cte_source__term.date_key as date_key
                from cte_source__datespine
                inner join
                    cte_source__term
                    on cte_source__datespine.date_key = cte_source__term.date_key
            )
        select *
        from cte_joined
    """
    columns = ["date_key"]
    lineage = ccl.extract_cll(sql, columns, debug=False)
    assert lineage is not None
    assert "date_key" in lineage

    assert "bds.bds_gen__datespine.date_key" in lineage["date_key"]
    assert "stg_student_mgt.stg_student_mgt__term.date_key" in lineage["date_key"]
