import pandas as pd


def execute_query(engine, query):

    df = pd.read_sql(query, engine)

    return df