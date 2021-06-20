class DataAccessError(Exception):
    """Error thrown when data access causes an error"""
    pass


class MissingArgumentError(Exception):
    """Error thrown when a method is missing a required argument"""  
    pass
