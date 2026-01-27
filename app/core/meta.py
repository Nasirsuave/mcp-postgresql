from .exec import exec_query

def get_current_schema():
    res = exec_query("SELECT current_schema()", None, 1, True)
    return res[0]["current_schema"] if res else "public"

def list_tables(schema: str):
    return exec_query(
        "SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = %s",
        [schema],
        10000,
        True
    )
