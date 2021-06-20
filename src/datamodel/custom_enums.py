from enum import Enum

class FileType(Enum):
    """Enum with file sheet types"""

    EXCEL = 'EXCEL'
    CSV = 'CSV'


class TaskType(Enum):
    """Enum with types of tasks"""

    IMPORT_CREATE = 'IMPORT_CREATE'
    IMPORT_EDIT = 'IMPORT_EDIT'
    BULK_EDIT = 'BULK_EDIT'


class ProductStatus(Enum):
    """Enum with product statuses"""

    ACTIVE = 'ACTIVE'
    DRAFT = 'DRAFT'
    ARCHIVED = 'ARCHIVED'


class JobStatus(Enum):
    SUBMITTED = 'SUBMITTED'
    PREPARING = 'PREPARING'
    RUNNING = 'RUNNING'
    COMPLETED = 'COMPLETED'
    PARTIAL_COMPLETE = 'PARTIALLY COMPLETED'
    FAILED = 'FAILED'