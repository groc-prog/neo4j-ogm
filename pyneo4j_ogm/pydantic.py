from pydantic import VERSION

IS_PYDANTIC_V2 = int(VERSION.split(".", 1)[0]) >= 2
