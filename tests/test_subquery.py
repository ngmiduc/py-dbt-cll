import json

import pytest

from py_dbt_cll.dbt_lineage import DbtCLL


@pytest.fixture
def ccl():
    with open("tests/manifest.json", "r", encoding="utf-8") as file:
        manifest_data = json.load(file)
    return DbtCLL(manifest_data)


def test_subquery(ccl):
    sql = """
        with
            cte_source__datespine as (select * from "master"."bds"."bds_gen__datespine"),
            cte_source__term as (
                select * from "master"."stg_student_mgt"."stg_student_mgt__term"
            ),
            cte_academic_terms as (
                select distinct academic_year_id, academic_year_start, academic_year_end
                from cte_source__term
            ),
            cte_joined as (
                select cte_source__datespine.*, cte_academic_terms.*
                from cte_source__datespine
                left join
                    cte_academic_terms
                    on cte_source__datespine.date_day
                    between cte_academic_terms.academic_year_start
                    and cte_academic_terms.academic_year_end
            ),
            cte_final as (
                select
                    format(cast(date_day as date), 'yyyy-MM-dd') as date_id,
                    day(date_day) as day_id,
                    datename(weekday, date_day) as day_label,
                    datepart(week, date_day) as week_id,
                    month(date_day) as month_id,
                    datename(month, date_day) as month_label,
                    datefromparts(year(date_day), month(date_day), 1) as month_start,
                    dateadd(
                        day,
                        -1,
                        dateadd(month, 1, datefromparts(year(date_day), month(date_day), 1))
                    ) as month_end,
                    year(date_day) as year_id,
                    datefromparts(year(date_day), 1, 1) as year_start,
                    dateadd(
                        day, -1, dateadd(year, 1, datefromparts(year(date_day), 1, 1))
                    ) as year_end,
                    academic_year_id as academic_year_id,
                    academic_year_start as academic_year_start,
                    academic_year_end as academic_year_end
                from cte_joined
            )
        select *
        from (
            select *
            from cte_final
        ) as final
    """
    columns = ["academic_year_id", "date_id"]
    lineage = ccl.extract_cll(sql, columns, debug=False)
    assert lineage is not None
    assert "academic_year_id" in lineage
    assert "date_id" in lineage

    assert (
        "stg_student_mgt.stg_student_mgt__term.academic_year_id"
        in lineage["academic_year_id"]
    )
    assert "bds.bds_gen__datespine.date_day" in lineage["date_id"]
