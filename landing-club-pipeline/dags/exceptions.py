class s3DataUploaderException(Exception):
    def __init__(self, msg):
        self.msg = msg


class snowflakeException(Exception):
    def __init__(self, msg):
        self.msg = msg


class ConfigError(Exception):
    def __init__(self, msg):
        self.errors = msg
