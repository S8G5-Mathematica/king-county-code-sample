import pandas as pd
import numpy as np
import os
import yaml
import snowflake.connector


def import_credentials():
    """
    Reads in the snowflake credentials for connection
    Returns a dictionary of credentials
    """
    progpath = os.getcwd()
    subfolder = progpath.split("\\")[-1]
    with open(
        os.path.join(
            progpath.replace(subfolder, "snowflake_credentials"), "config_cred.yml"
        )
    ) as f:
        config = yaml.safe_load(f)
    return config


def snowflake_con(config, role="NKFM_PROD"):
    """
    Uses credentials to connect to snowflake
    Returns connection and cursor
    """
    ctx = snowflake.connector.connect(
        user=config["user"], password=config["password"], account=config["account"]
    )
    cs = ctx.cursor()
    cs.execute("USE ROLE {}".format(role))
    cs.execute("USE WAREHOUSE NKFM_PROD_WH")
    cs.execute("USE DATABASE NKFM_PROD")
    cs.execute("USE SCHEMA MDHHS")
    return ctx, cs


def close_con(ctx, cs):
    """
    Closes the snowflake connection
    Returns nothing
    """
    cs.close()
    ctx.close()


def write_out_table(df, title, f):
    """
    Writes a table to the output file with a title
    Returns nothing
    """
    f.write("{}\n".format(title))
    table_str = df.to_string(header=True, index=False)
    f.write(table_str + "\n\n")


def upload_to_temp_table(table_name, value_list, vars, cs):
    """
    Uploads a value list (prepared string) to a temporary database table
    Returns nothing
    """
    cs.execute(
        f"""
            create temp table {table_name} as 
                select 
                    *
                from
                (values {value_list}) x({vars})
    """
    )


def freq_query(var, table, ctx, count="count(*)", count_var="n", where="",
    addtl_count="", var_select=""):
    """
    Gets a frequency of a database table
    Returns a dataframe
    """
    var_select = np.where(var_select == "", var, var_select)
    where_command = def_where_command(where)
    return pd.read_sql(
        f"""select
                    {var_select}, {count} as {count_var} {addtl_count}
                    from {table}
                    {where_command}
                    group by {var}
                    order by {var}
                    """,
        con=ctx,
    )


def distribution_query(var, table, ctx, group_by="", group_by_var=""):
    """
    Gets full distribution of a variable
    Returns a dataframe
    """
    return pd.read_sql(
        f"""select 
                     '{var}' as variable
                    {group_by_var}
                    ,avg({var}) as mean
                    ,min({var}) as min
                    ,percentile_cont(.01) WITHIN GROUP (ORDER BY {var}) AS prcntl_1
                    ,percentile_cont(.05) WITHIN GROUP (ORDER BY {var}) AS prcntl_5
                    ,percentile_cont(.10) WITHIN GROUP (ORDER BY {var}) AS prcntl_10
                    ,percentile_cont(.25) WITHIN GROUP (ORDER BY {var}) AS prcntl_25
                    ,median({var}) as prcntl_50
                    ,percentile_cont(.75) WITHIN GROUP (ORDER BY {var}) AS prcntl_75
                    ,percentile_cont(.95) WITHIN GROUP (ORDER BY {var}) AS prcntl_95
                    ,percentile_cont(.99) WITHIN GROUP (ORDER BY {var}) AS prcntl_99
                    ,max({var}) as max
                    from {table}
                    {group_by}""",
        con=ctx,
    )


def count_total(table, ctx, count="count(*)", where=""):
    """
    Finds count from table
    Returns a number
    """
    where_command = def_where_command(where)
    table = pd.read_sql(
        f"""select
                    {count} as n
                    from {table}
                    {where_command}
                    """,
        con=ctx,
    )
    return table.iloc[0, 0]


def get_cat_list(var, table, ctx, where=""):
    """
    Gets all unique values of a variable from a table
    Returns a list
    """
    where_command = def_where_command(where)
    df = pd.read_sql(
        f"""select 
                {var}
                from {table}
                {where_command}
                group by {var}
                order by {var}""",
        con=ctx,
    )
    upcase_var = var.upper()
    return df[upcase_var].tolist()


def def_where_command(where):
    """
    Defines if where command is present
    Returns a string
    """
    return np.where(where == "", "", f"where {where}")
