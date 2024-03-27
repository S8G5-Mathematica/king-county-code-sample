import pandas as pd
import numpy as np
import sys
from datetime import date
import re
import argparse
import utilities as util


def process_arguments(args):
    """
    Processes command line arguments provided to main.py
    Returns a dictionary of arguments
    """
    parser = argparse.ArgumentParser(description="Set for testing")

    # ... provide arguments here ...
    parser.add_argument(
        "-yr",
        "--year",
        dest="year",
        required=True,
        action="store",
        help="Year of analysis (formatted yyyy)",
    )
    parser.add_argument(
        "-t",
        "--test",
        dest="test_run",
        action="store_true",
        help="Include this argument if running a test run on a sample of observations",
    )
    parser.add_argument(
        "-d",
        "--diag",
        dest="diagnostics",
        action="store_true",
        help="Include this argument to run diagnostic checks and adhoc investigations",
    )

    try:
        assert len(args) > 0
    except:
        parser.print_help()
        sys.exit(2)

    inputs = parser.parse_args(args)
    return validate_arguments(inputs)


def validate_arguments(options):
    """
    Processes and checks input arguments
    Returns a dictionary of arguments
    """
    inputs = vars(options)

    inputs["select_obs"], inputs["test_name"] = testing_obs(inputs["test_run"])

    try:
        assert re.search("\d{4}", inputs["year"]) != None
    except:
        raise ValueError("The year is not in a valid format (yyyy)")
    return inputs


def testing_obs(test_run):
    """
    Assigns values based on whether the test run argument is present
    Returns two strings
    """
    return np.where(test_run, "top 10000", ""), np.where(test_run, "test_", "")


def cond_flags(input_args, prev_year, cs, ctx, f):
    """
    Assigns values based on whether the test run argument is present
    Returns nothing (but creates a temporary member-level table with necessary flags)
    """
    cs.execute(
        f"""
        create or replace temp table jvhl as
            select member_id
                  ,requestcpt
                  ,case when numericresult = 'NULL' then null 
                        else to_number(numericresult) end as numericresult
                  ,textresult
                  ,case when rlike(textresult, '>/?=? ?(60|90|120).?0?0?') 
                            then to_number(replace(replace(replace(textresult,'/',''),'=',''),'>',''))
                        when numericresult != 'NULL' then to_number(numericresult)
                        else null end as all_results
                  ,DATE_SERVICEBEGIN
            from nkfm_prod.jvhl.labresults
            where year(DATE_SERVICEBEGIN) in ({input_args['year']}, {prev_year})
    """
    )
    print(
        util.freq_query("numericresult,all_results", "jvhl", ctx, where="requestcpt='33914-3'")
    )
    print(
        util.freq_query("textresult,all_results", "jvhl", ctx, where="requestcpt='33914-3'")
    )
    cs.execute(
        f"""
        create or replace temp table jvhl_flags as
            select member_id
                  ,case when datediff(day, min(DATE_SERVICEBEGIN), max(DATE_SERVICEBEGIN)) >= 90 then 1
                        else 0 end as ckd_jvhl_flag_2labs
            from jvhl
            where requestcpt = '33914-3' and numericresult < 60
            group by member_id
    """
    )
    cs.execute(
        f"""
        create or replace temp table member_jvhl as
            select e.member_id
                  ,max(case when e.mhp in ('AET','BCC','HAP','MCL','MER') then 1 
                            else 0 end) as jvhl_denom_flag
                  ,max(case when l.member_id is not null then 1 
                            else 0 end) as ckd_jvhl_flag
                  ,max(coalesce(l.ckd_jvhl_flag_2labs,0)) as ckd_jvhl_flag_2labs
                  ,case when max(f.esrd) = 1 then 0 
                        else ckd_jvhl_flag end as ckd_no_esrd_jvhl_flag
                  ,case when max(f.esrd) = 1 then 0 
                        else max(f.ckd) end as ckd_no_esrd_flag
                  ,max(f.ckd) as ckd_ccw_flag
                  ,max(f.esrd) as esrd_flag
                  ,max(f.aki) as aki_flag_{input_args['year']}
                  ,max(coalesce(p.aki,0)) as aki_flag_{prev_year}
                  ,max(m.age) as age
            from math_prod.common.enroll_{input_args['year']} as e
                left join jvhl_flags as l
                    on e.member_id = l.member_id
                left join math_prod.common.condition_flags_{input_args['year']} as f
                    on e.member_id = f.member_id
                left join math_prod.common.condition_flags_{prev_year} as p
                    on e.member_id = p.member_id
                left join math_prod.common.member_{input_args['year']} as m
                    on e.member_id = m.member_id
            group by e.member_id
    """
    )
    print(
        util.freq_query(
            "jvhl_denom_flag,ckd_jvhl_flag,ckd_jvhl_flag_2labs,ckd_no_esrd_jvhl_flag",
            "member_jvhl",
            ctx,
        )
    )
    print(util.freq_query("ckd_jvhl_flag,ckd_no_esrd_jvhl_flag", "member_jvhl", ctx))
    print(util.freq_query("ckd_jvhl_flag,ckd_jvhl_flag_2labs", "member_jvhl", ctx))
    ckd_esrd = util.count_total(
        "member_jvhl", ctx, where="ckd_jvhl_flag=1"
    ) - util.count_total("member_jvhl", ctx, where="ckd_no_esrd_jvhl_flag=1")
    f.write(f"Beneficiaries flagged as CKD from JVHL data and ESRD: {ckd_esrd}\n\n")


def ckd_stage_lab(cs, ctx):
    """
    Assigns CKD stage to members based on most recent (or highest) lab result
    Returns nothing (but creates a temporary member-level table with lab CKD flags)
    """
    # max date result from JVHL
    cs.execute(
        """
        create or replace temp table jvhl_date as
            select member_id
                  ,max(DATE_SERVICEBEGIN) as max_date
            from jvhl
            where requestcpt = '33914-3' and all_results is not null
            group by member_id
    """
    )
    # taking min of results (for highest stage) from most recent day
    cs.execute(
        """
        create or replace temp table jvhl_maxdate_result as
            select a.member_id
                  ,a.DATE_SERVICEBEGIN
                  ,min(a.all_results) as all_results
                  ,count(distinct a.all_results) as n_labs
            from jvhl as a
                inner join jvhl_date as b
                    on a.member_id = b.member_id
                    and a.DATE_SERVICEBEGIN = b.max_date
                    and a.requestcpt = '33914-3'
            group by a.member_id, a.DATE_SERVICEBEGIN
    """
    )
    print(util.freq_query("n_labs", "jvhl_maxdate_result", ctx))


def ckd_stage_claims(input_args, prev_year, cs, ctx):
    """
    Assigns CKD stage to members based on most recent (or highest) claims result
    Returns nothing (but creates a temporary member-level table with lab claims flags)
    """
    # max date result from claims
    cs.execute(
        f"""
        create or replace temp table dx_date as
            select a.*
                ,b.from_date
                ,case when len(a.dx_code) = 4 then to_number(concat(right(a.dx_code,3),'0'))
                    when len(a.dx_code) = 5 then to_number(right(a.dx_code,4))
                    else null end as dx_num
                from (
                select * from math_prod.common.claims_dx_long_{input_args['year']} 
                    union
                select * from math_prod.common.claims_dx_long_{prev_year}
                ) as a
                left join nkfm_prod.mdhhs.nkfm_claims as b
                    on a.claim_id = b.claim_id
                where a.dx_code in ('N181','N182','N183','N1830','N1831','N1832','N184','N185')
    """
    )
    cs.execute(
        """
        create or replace temp table dx_ckd_date as 
            select member_id
                ,max(from_date) as max_date
            from dx_date
            group by member_id
    """
    )
    # taking max of diagnosis codes (for highest stage) from most recent day
    cs.execute(
        """
        create or replace temp table dx_maxdate_result as
            select a.member_id
                  ,max(dx_num) as dx_num
                  ,b.from_date
            from dx_ckd_date as a
                inner join dx_date as b
                    on a.member_id = b.member_id
                    and a.max_date = b.from_date
            group by a.member_id, b.from_date
    """
    )
    # look for most recent specific stage 3 (or highest stage if tie)
    cs.execute(
        """
        create or replace temp table stage3_members as
            select a.member_id
                ,b.dx_num
                ,b.from_date
            from dx_maxdate_result as a
                left join dx_date as b
                on a.member_id = b.member_id
            where a.dx_num = 1830 and b.dx_num in (1831,1832)
    """
    )
    cs.execute(
        """
        create or replace temp table stage3_date as 
            select member_id
                ,max(from_date) as max_date
            from stage3_members
            group by member_id
    """
    )
    cs.execute(
        """
        create or replace temp table stage3_maxdate_result as
            select a.member_id
                  ,max(b.dx_num) as dx_num
                  ,b.from_date
            from stage3_date as a
                inner join dx_date as b
                    on a.member_id = b.member_id
                    and a.max_date = b.from_date
                    and b.dx_num in (1831,1832)
            group by a.member_id, b.from_date
    """
    )
    # recombine with all unspecified stage 3
    cs.execute(
        """
        create or replace temp table stage3_all as
            select a.member_id
                  ,case when b.dx_num is null then a.dx_num 
                        else b.dx_num end as dx_num
                  ,case when b.from_date is null then a.from_date 
                        else b.from_date end as from_date
            from dx_maxdate_result as a
                left join stage3_maxdate_result as b
                    on a.member_id = b.member_id
            where a.dx_num = 1830
    """
    )
    # recombine with all other diagnoses
    cs.execute(
        """
        create or replace temp table dx_date_result as
            select * from stage3_all
            union
            select * from dx_maxdate_result where dx_num != 1830
    """
    )
    assert util.count_total("dx_maxdate_result", ctx) == util.count_total(
        "dx_date_result", ctx
    )


def stage_flags(input_args, prev_year, cs, ctx, f):
    """
    Assigns single CKD stage to members based on most recent (or highest)
        CKD stage from labs and claims
    Returns nothing (but creates a temporary member-level table with final CKD flags)
    """
    ckd_stage_lab(cs, ctx)
    ckd_stage_claims(input_args, prev_year, cs, ctx)

    # define stages
    cs.execute(
        """
        create or replace temp table member_jvhl_stage as
            select m.*
                  ,case when l.all_results >= 60 then 'stage 1/2'
                        when l.all_results >= 45 and l.all_results < 60 then 'stage 3a'
                        when l.all_results >= 30 and l.all_results < 45 then 'stage 3b'
                        when l.all_results >= 15 and l.all_results < 30 then 'stage 4'
                        when l.all_results < 15 then 'stage 5'
                        else '0' end as ckd_stage_jvhl
                  ,case when l.all_results >= 90 then 'stage 1'
                        when l.all_results >= 60 and l.all_results < 90 then 'stage 2'
                        when l.all_results >= 45 and l.all_results < 60 then 'stage 3a'
                        when l.all_results >= 30 and l.all_results < 45 then 'stage 3b'
                        when l.all_results >= 15 and l.all_results < 30 then 'stage 4'
                        when l.all_results < 15 then 'stage 5'
                        else '0' end as ckd_stage_jvhl_detailed
                  ,case when d.dx_num = 1810 then 'stage 1'
                        when d.dx_num = 1820 then 'stage 2'
                        when d.dx_num = 1830 then 'stage 3, unspecified'
                        when d.dx_num = 1831 then 'stage 3a'
                        when d.dx_num = 1832 then 'stage 3b'
                        when d.dx_num = 1840 then 'stage 4'
                        when d.dx_num = 1850 then 'stage 5'
                        when m.ckd_ccw_flag = 1 and d.dx_num is null then '0'
                        else '' end as ckd_stage_claims
                  ,d.from_date as ckd_stage_claims_date
                  ,l.date_servicebegin as ckd_stage_jvhl_date
                  ,case when l.date_servicebegin > d.from_date 
                            or (ckd_stage_claims in ('','0','stage 3, unspecified') 
                                and ckd_stage_jvhl_detailed != '0') 
                            then 'lab'
                        when d.from_date >= l.date_servicebegin 
                            or (ckd_stage_claims not in ('','0') 
                                and ckd_stage_jvhl_detailed = '0') 
                            then 'claim'
                        else '' end as recent_stage
                  ,case when recent_stage = 'lab' then ckd_stage_jvhl_detailed
                        when recent_stage = 'claim' then ckd_stage_claims
                        else '' end as ckd_stage
                  ,case when m.ckd_ccw_flag = 1 or m.ckd_jvhl_flag = 1 then 1 
                        else 0 end as ckd_ccw_lab_flag
            from member_jvhl as m
                left join jvhl_maxdate_result as l
                    on m.member_id = l.member_id
                left join dx_date_result as d
                    on m.member_id = d.member_id
    """
    )
    print(
        util.freq_query("ckd_stage_jvhl,ckd_stage_jvhl_detailed", "member_jvhl_stage", ctx)
    )
    print(
        util.freq_query("ckd_stage_jvhl_detailed,ckd_stage_claims", "member_jvhl_stage", ctx)
    )

    print(
        pd.read_sql("""select * from member_jvhl_stage 
                    where ckd_stage_claims != '0' 
                        and ckd_stage_claims != '' 
                        and ckd_stage_jvhl_detailed != '0' 
                    order by member_id limit 20""",
            con=ctx,
        )
    )
    print(
        util.freq_query(
            "recent_stage,ckd_stage", "member_jvhl_stage", ctx,
            where="ckd_stage_jvhl_detailed != ckd_stage_claims",
        )
    )

    print(
        util.freq_query(
            "ckd_stage_jvhl_detailed,ckd_ccw_lab_flag", "member_jvhl_stage", ctx
        )
    )
    print(
        util.freq_query("ckd_stage_claims,ckd_ccw_lab_flag", "member_jvhl_stage", ctx)
    )
    print(
        util.freq_query("ckd_stage_claims,ckd_ccw_lab_flag", "member_jvhl_stage", ctx)
    )
    cs.execute(
        f"""
        create or replace temp table member_jvhl_stage_comb as
            select *
                  ,case when ESRD_flag = 1 then 'ESRD'
                        when ckd_ccw_lab_flag = 1 and ckd_stage_claims in ('0','') 
                                and ckd_stage_jvhl_detailed in ('0','') 
                                then 'CKD, stage unknown'
                        when ckd_ccw_lab_flag = 1 and ckd_stage = 'stage 5' 
                             then 'stage 5'
                        when ckd_ccw_lab_flag = 1 and ckd_stage = 'stage 4' 
                             then 'stage 4'
                        when ckd_ccw_lab_flag = 1 and ckd_stage = 'stage 3b' 
                             then 'stage 3b'
                        when ckd_ccw_lab_flag = 1 and ckd_stage = 'stage 3a' 
                             then 'stage 3a'
                        when ckd_stage = 'stage 2' then 'stage 2'
                        when ckd_stage = 'stage 1' then 'stage 1'
                        when ckd_ccw_lab_flag = 1 
                                and ckd_stage_claims = 'stage 3, unspecified' 
                                then 'CKD, stage 3 unspecified'
                        when ckd_ccw_lab_flag = 0 
                                and jvhl_denom_flag = 0 
                                then 'No lab or claim for CKD, not in JVHL denom'
                        when ckd_ccw_lab_flag = 0 
                                and jvhl_denom_flag = 1 
                                then 'No lab or claim for CKD, in JVHL denom'
                        else '' end as ckd_stage_comb_all
                  ,case when esrd_flag = 1 or 
                            (ckd_ccw_lab_flag = 1 and ckd_stage = 'stage 5') 
                            then 'stage 5/ESRD'
                        when ckd_ccw_lab_flag = 1 and ckd_stage_claims in ('','0') 
                            and ckd_stage_jvhl_detailed in ('stage 1','stage 2','','0') 
                            then 'CKD, stage unknown'
                        when ckd_ccw_lab_flag = 1 and ckd_stage = 'stage 4' 
                            then 'stage 4'
                        when ckd_ccw_lab_flag = 1 and ckd_stage = 'stage 3b' 
                            then 'stage 3b'
                        when ckd_ccw_lab_flag = 1 and ckd_stage = 'stage 3a' 
                            then 'stage 3a'
                        when ckd_stage = 'stage 2' and recent_stage = 'claim' 
                            then 'stage 2'
                        when ckd_stage = 'stage 1' and recent_stage = 'claim' 
                            then 'stage 1'
                        when ckd_ccw_lab_flag = 1 
                            and ckd_stage_claims = 'stage 3, unspecified' 
                            and ckd_stage_jvhl_detailed in ('0','') 
                            then 'stage 3 unspecified'
                        else 'No lab or claim for CKD' end as ckd_stage_comb_w3unsp
            from member_jvhl_stage
    """
    )
    cs.execute(
        f"""
        create or replace temp table final_flags as
            select *
                  ,case when ckd_stage_comb_all in ('ESRD','stage 5') 
                            then 'stage 5/ESRD'
                        when ckd_stage_comb_all in (
                            'CKD, stage unknown','CKD, stage 3 unspecified'
                            ) then 'CKD, stage unknown/unspecified'
                        when ckd_stage_comb_all in (
                            'No lab or claim for CKD, not in JVHL denom',
                            'No lab or claim for CKD, in JVHL denom'
                            ) then 'No lab or claim for CKD'
                        else ckd_stage_comb_all end as ckd_stage_comb_5andesrd
                  ,case when ckd_stage_comb_w3unsp in (
                            'No lab or claim for CKD','stage 1','stage2'
                            ) then 'stage 1, stage 2, or no CKD'
                        when ckd_stage_comb_w3unsp in (
                            'stage 3 unspecified','CKD, stage unknown'
                            ) then 'CKD, stage unknown/unspecified'
                        else ckd_stage_comb_w3unsp end as ckd_stage_comb_5cat
            from member_jvhl_stage_comb
    """
    )
    print(
        util.freq_query("ckd_stage_comb_all,ckd_stage_comb_w3unsp", "final_flags", ctx)
    )

    df1 = util.freq_query("ckd_stage_comb_all", "final_flags", ctx)
    util.write_out_table(df1, f"ckd_stage_comb_all for {input_args['year']}", f)
    df2 = util.freq_query("ckd_stage_comb_5andesrd", "final_flags", ctx)
    util.write_out_table(df2, f"ckd_stage_comb_5andesrd for {input_args['year']}", f)
    df3 = util.freq_query("ckd_stage_comb_w3unsp", "final_flags", ctx)
    util.write_out_table(df3, f"ckd_stage_comb_w3unsp for {input_args['year']}", f)
    df4 = util.freq_query("ckd_stage_comb_5cat", "final_flags", ctx)
    util.write_out_table(df4, f"ckd_stage_comb_5cat for {input_args['year']}", f)


def diagnostics(input_args, prev_year, cs, ctx, f):
    """
    Outputs frequencies and averages costs by CKD stages
    Returns nothing (but populates output file)
    """
    # frequencies and crosstabs
    for cond in ["adults", "adults in JVHL denominator"]:
        where_statement = np.where(
            cond == "adults", "age >= 18", "age >= 18 and jvhl_denom_flag = 1"
        )
        for var in [
            "ckd_jvhl_flag",
            "ckd_stage_jvhl",
            "ckd_jvhl_flag_2labs",
            "ckd_stage_jvhl_detailed",
            "ckd_stage_comb_all",
            "ckd_stage_comb_5andesrd",
            "ckd_stage_claims",
            "ckd_stage_comb_w3unsp",
            "ckd_stage_comb_5cat",
        ]:
            df = util.freq_query(var, "final_flags", ctx, where=where_statement)
            util.write_out_table(df, f"{var} for {cond} in {input_args['year']}", f)
            if var == "ckd_jvhl_flag":
                for var2 in ["ckd_ccw_flag", "ckd_jvhl_flag_2labs", 
                             f"aki_flag_{input_args['year']}, aki_flag_{prev_year}"]:
                    df = util.freq_query(
                        f"{var},{var2}", "final_flags", ctx, where=where_statement
                    )
                    util.write_out_table(df,f"{var} and {var2} crosstab for {cond} in {input_args['year']}",f)
            elif var == "ckd_stage_jvhl_detailed":
                for var2 in ["ckd_stage_claims", "ckd_stage_claims,ckd_stage_comb_all"]:
                    df = util.freq_query(f"{var},{var2}", "final_flags", ctx, where=where_statement)
                    util.write_out_table(
                        df, f"{var} and {var2} crosstab for {cond} in {input_args['year']}", f
                    )
            elif var == "ckd_stage_claims":
                for var2 in ["esrd_flag,ckd_ccw_flag"]:
                    df = util.freq_query(f"{var},{var2}", "final_flags", ctx, where=where_statement)
                    util.write_out_table(
                        df, f"{var} and {var2} crosstab for {cond} in {input_args['year']}", f
                    )
    # labs and claims by month
    df_clm = util.freq_query(
        "to_varchar(ckd_stage_claims_date,'yyyyMM')",
        "final_flags",
        ctx,
        var_select="to_varchar(ckd_stage_claims_date,'yyyyMM') as month",
    )
    df_jvhl = util.freq_query(
        "to_varchar(ckd_stage_jvhl_date,'yyyyMM')",
        "final_flags",
        ctx,
        var_select="to_varchar(ckd_stage_jvhl_date,'yyyyMM') as month",
    )
    df_clm.rename(columns={"N": "ckd_stage_claims_date"}, inplace=True)
    df_jvhl.rename(columns={"N": "ckd_jvhl_claims_date"}, inplace=True)
    df = pd.merge(df_clm, df_jvhl, how="outer", on="MONTH")
    util.write_out_table(
        df, f"Monthly counts of claims and labs in {input_args['year']}", f
    )
    # average cost by categories of beneficiaries and claim types
    cs.execute(
        f"""
        create or replace temp table claim_prep as
            select {input_args['select_obs']} c.claim_id
                  ,c.member_id
                  ,max(c.from_date) as from_date
                  ,case when lower(f.fasc_cat) = 'inpatient' 
                            then max(ip.allowed_amount)
                        else sum(c.allowed_amt) end as allowed_amt
                  ,case when lower(f.fasc_cat) in ('inpatient','clinic','drug',
                            'op facility','nf') then f.fasc_cat
                        else 'other' end as fasc_cat_adj
            from math_prod.common.claims_{input_args['year']} as c
                inner join math_prod.imputation.fasc_{input_args['year']} as f
                    on c.claim_id = f.claim_id
                left join math_dev.imputation.ip_header_imputed_{input_args['year']} as ip
                    on c.claim_id = ip.claim_id
            where c.claim_status != 1
            group by c.claim_id, c.member_id, f.fasc_cat
    """
    )
    title = "Cost per bene year by stage"
    df = clm_sum(input_args["year"], cs, ctx)
    util.write_out_table(df, f"{title} for {input_args['year']}", f)
    for cat in ["inpatient", "clinic", "op facility", "nf", "other"]:
        df = clm_sum(input_args["year"], cs, ctx, fasc_cat=cat)
        util.write_out_table(df, f"{title} for {cat} FASC category for {input_args['year']}", f)
    df = clm_sum(input_args["year"], cs, ctx, med_flag="Y", dual_flag="N")
    util.write_out_table(df, f"{title} where med_flag='Y' and dual_flag='N' for {input_args['year']}", f)
    df = clm_sum(input_args["year"], cs, ctx, dual_flag="Y")
    util.write_out_table(df, f"{title} where dual_flag='Y' for {input_args['year']}", f)


def flag_where(value):
    """
    Sets value for medical and dual flags using "both" option
    Returns string
    """
    if value == "both":
        return "in ('Y','N')"
    else:
        return f"= '{value}'"


def clm_sum(year, cs, ctx, fasc_cat="all", med_flag="both", dual_flag="both"):
    """
    Calculates average cost by stage for specified group of people or claims
    Returns dataframe
    """
    fasc_where = np.where(fasc_cat == "all", "!= 'drug'", f"= '{fasc_cat}'")
    cs.execute(
        f"""
    create or replace temp table cost_sum as
            select e.member_id
                  ,max(f.ckd_stage_jvhl_detailed) as ckd_stage_jvhl_detailed
                  ,max(f.ckd_stage_claims) as ckd_stage_claims
                  ,max(f.ckd_stage_comb_all) as ckd_stage_comb_all
                  ,max(f.ckd_stage_comb_5andesrd) as ckd_stage_comb_5andesrd
                  ,max(f.ckd_no_esrd_flag) as ckd_no_esrd_flag
                  ,max(f.ckd_stage_comb_w3unsp) as ckd_stage_comb_w3unsp
                  ,max(f.ckd_stage_comb_5cat) as ckd_stage_comb_5cat
                  ,sum(c.allowed_amt) as cost_sum
                  ,max(e.n_month) / 12 as n_year
            from (
                    select e.member_id
                          ,to_varchar(e.begin_date, 'yyyyMM') as month
                          ,count(*) over (partition by e.member_id) as n_month
                    from math_prod.common.enroll_{year} as e
                        left join math_prod.common.member_{year} as m
                            on e.member_id = m.member_id
                    where e.medical_flag {flag_where(med_flag)} 
                        and e.dual_flag {flag_where(dual_flag)} 
                        and m.age >= 18
                    ) as e 
            left join (
                    select allowed_amt, member_id, from_date
                    from claim_prep
                    where lower(fasc_cat_adj) {fasc_where}
                    ) as c
                on c.member_id = e.member_id
                and to_varchar(c.from_date, 'yyyyMM') = e.month
            left join final_flags as f
                on e.member_id = f.member_id
            group by e.member_id
    """
    )
    df_all = pd.DataFrame()
    for var in ["ckd_stage_jvhl_detailed", "ckd_stage_claims", "ckd_stage_comb_all",
                "ckd_stage_comb_5andesrd", "ckd_stage_comb_w3unsp", "ckd_stage_comb_5cat"]:
        where_statement = np.where(var != "ckd_stage_claims", 
                                   "", 
                                   "where ckd_no_esrd_flag = 1")
        df = pd.read_sql(
            f"""
            select '{var}' as stage_var
                   ,{var} as stage
                   ,sum(cost_sum) as total_cost
                   ,sum(n_year) as n_year
                   ,sum(cost_sum) / sum(n_year) as cost_per_bene_yr
            from cost_sum
            {where_statement}
            group by {var}
            order by {var}
        """,
            con=ctx,
        )
        df_all = pd.concat([df_all, df])
    return df_all


def main():
    input_args = process_arguments(sys.argv[1:])
    output_date = np.where(input_args["test_run"], "", "_{}".format(str(date.today())))

    f = open(
        f"output/{input_args['test_name']}ckd_lab_claims_diagnostics_{input_args['year']}{output_date}.txt",
        "w",
    )

    config = util.import_credentials()
    ctx, cs = util.snowflake_con(config, role="SYSADMIN")

    prev_year = str(int(input_args["year"]) - 1)
    cond_flags(input_args, prev_year, cs, ctx, f)
    stage_flags(input_args, prev_year, cs, ctx, f)

    if input_args["diagnostics"]:
        diagnostics(input_args, prev_year, cs, ctx, f)

    util.close_con(ctx, cs)
    f.close()


if __name__ == "__main__":
    main()
