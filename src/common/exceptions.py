
class PipelineException(Exception):
    """Base exception for pipeline-related errors."""


class ConfigurationException(PipelineException):
    """Raised when pipeline configuration is invalid."""


class DataIngestionException(PipelineException):
    """Raised when source data ingestion fails."""


class DataTransformationException(PipelineException):
    """Raised when data transformation fails."""


class DataWriteException(PipelineException):
    """Raised when writing pipeline output fails."""
