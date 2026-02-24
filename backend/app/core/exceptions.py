"""
Custom exceptions for the application.

Defines hierarchy of application-specific exceptions for
clean error handling and proper HTTP status code mapping.
"""

from typing import Any, Dict, Optional


class RosettaException(Exception):
    """
    Base exception for all application errors.

    All custom exceptions should inherit from this class.
    """

    def __init__(
        self,
        message: str,
        status_code: int = 500,
        details: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize exception.

        Args:
            message: Human-readable error message
            status_code: HTTP status code to return
            details: Additional error context
        """
        self.message = message
        self.status_code = status_code
        self.details = details or {}
        super().__init__(self.message)

    def to_dict(self) -> Dict[str, Any]:
        """Convert exception to dictionary for API response."""
        return {
            "error": self.__class__.__name__,
            "message": self.message,
            "details": self.details,
        }


class DatabaseError(RosettaException):
    """
    Database operation error.

    Raised when a database operation fails (query, insert, update, delete).
    """

    def __init__(
        self,
        message: str = "Database operation failed",
        details: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(message=message, status_code=500, details=details)


class DatabaseConnectionError(DatabaseError):
    """
    Database connection error.

    Raised when unable to establish or maintain database connection.
    """

    def __init__(
        self,
        message: str = "Failed to connect to database",
        details: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(message=message, details=details)


class EntityNotFoundError(RosettaException):
    """
    Entity not found error.

    Raised when a requested entity does not exist in the database.
    """

    def __init__(
        self, entity_type: str, entity_id: Any, details: Optional[Dict[str, Any]] = None
    ):
        message = f"{entity_type} with id '{entity_id}' not found"
        details = details or {}
        details.update(
            {
                "entity_type": entity_type,
                "entity_id": str(entity_id),
            }
        )
        super().__init__(message=message, status_code=404, details=details)


class ValidationError(RosettaException):
    """
    Input validation error.

    Raised when request data fails validation.
    """

    def __init__(
        self,
        message: str = "Validation failed",
        details: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(message=message, status_code=422, details=details)


class DuplicateEntityError(RosettaException):
    """
    Duplicate entity error.

    Raised when attempting to create an entity that already exists.
    """

    def __init__(
        self,
        entity_type: str,
        field: str,
        value: Any,
        details: Optional[Dict[str, Any]] = None,
    ):
        message = f"{entity_type} with {field}='{value}' already exists"
        details = details or {}
        details.update(
            {
                "entity_type": entity_type,
                "field": field,
                "value": str(value),
            }
        )
        super().__init__(message=message, status_code=409, details=details)


class PipelineOperationError(RosettaException):
    """
    Pipeline operation error.

    Raised when a pipeline operation (start, pause, refresh) fails.
    """

    def __init__(
        self,
        pipeline_id: int,
        operation: str,
        reason: str,
        details: Optional[Dict[str, Any]] = None,
    ):
        message = f"Failed to {operation} pipeline {pipeline_id}: {reason}"
        details = details or {}
        details.update(
            {
                "pipeline_id": pipeline_id,
                "operation": operation,
                "reason": reason,
            }
        )
        super().__init__(message=message, status_code=400, details=details)


class WALMonitorError(RosettaException):
    """
    WAL monitoring error.

    Raised when WAL size monitoring fails.
    """

    def __init__(
        self,
        source_id: int,
        message: str = "Failed to monitor WAL size",
        details: Optional[Dict[str, Any]] = None,
    ):
        details = details or {}
        details.update({"source_id": source_id})
        super().__init__(message=message, status_code=500, details=details)


class ConfigurationError(RosettaException):
    """
    Configuration error.

    Raised when application configuration is invalid.
    """

    def __init__(
        self,
        message: str = "Invalid configuration",
        details: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(message=message, status_code=500, details=details)


class AuthenticationError(RosettaException):
    """
    Authentication error.

    Raised when authentication fails.
    """

    def __init__(
        self,
        message: str = "Authentication failed",
        details: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(message=message, status_code=401, details=details)


class AuthorizationError(RosettaException):
    """
    Authorization error.

    Raised when user lacks permission for requested operation.
    """

    def __init__(
        self,
        message: str = "Insufficient permissions",
        details: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(message=message, status_code=403, details=details)


class ExternalServiceError(RosettaException):
    """
    External service error.

    Raised when communication with external service fails.
    """

    def __init__(
        self,
        service: str,
        message: str = "External service error",
        details: Optional[Dict[str, Any]] = None,
    ):
        details = details or {}
        details.update({"service": service})
        super().__init__(message=message, status_code=502, details=details)
