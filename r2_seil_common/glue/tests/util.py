from awsglue.dynamicframe import DynamicFrame


def cmpData(left: DynamicFrame, right: DynamicFrame) -> bool:
    """Return True if data of left == data of right DynamicFrame"""
    data1 = left.toDF().collect()
    data2 = right.toDF().collect()
    return set(data1) == set(data2)


def cmpSchema(
    left: DynamicFrame, right: DynamicFrame, checkNullable: bool = False
) -> bool:
    """Return True if schema of left == schema of right DynamicFrame"""

    field_list = lambda fields: (fields.name, fields.dataType, fields.nullable)
    fields1 = [*map(field_list, left.toDF().schema.fields)]
    fields2 = [*map(field_list, right.toDF().schema.fields)]
    if checkNullable:
        res = set(fields1) == set(fields2)
    else:
        res = set([field[:-1] for field in fields1]) == set(
            [field[:-1] for field in fields2]
        )
    return res
