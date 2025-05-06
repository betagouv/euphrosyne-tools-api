import enum


@enum.unique
class VMSizes(str, enum.Enum):
    IMAGERY = "IMAGERY"
    IMAGERY_LARGE = "IMAGERY_LARGE"
