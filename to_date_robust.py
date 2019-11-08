from pyspark.sql import functions
from dateutil import parser


def to_iso_string(x):
    try:
        return parser.parse(x).isoformat()
    except:
        return None


to_iso_string_udf = functions.udf(to_iso_string)


def to_date_robust(x):
    return functions.to_date(to_iso_string_udf(x))
