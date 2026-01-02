"""
Centralized Error Handling for Retrieval Studio
Provides user-friendly error messages and recovery suggestions
"""
from typing import Optional
import traceback


class RetrievalStudioError(Exception):
    """Base exception for Retrieval Studio"""
    pass


class ConfigurationError(RetrievalStudioError):
    """Configuration-related errors"""
    pass


class JobSubmissionError(RetrievalStudioError):
    """Job submission errors"""
    pass


class ConnectionError(RetrievalStudioError):
    """Connection errors"""
    pass


def format_error_message(error: Exception, context: str = "") -> str:
    """
    Format error message for user display
    
    Args:
        error: The exception that occurred
        context: Additional context about where the error occurred
    
    Returns:
        Formatted error message
    """
    error_type = type(error).__name__
    error_msg = str(error)
    
    # Add context if provided
    if context:
        base_msg = f"{context}: {error_msg}"
    else:
        base_msg = error_msg
    
    # Add recovery suggestions based on error type
    suggestions = get_recovery_suggestions(error_type, error_msg)
    
    if suggestions:
        return f"{base_msg}\n\n**Suggestions:**\n" + "\n".join(f"- {s}" for s in suggestions)
    
    return base_msg


def get_recovery_suggestions(error_type: str, error_msg: str) -> list:
    """Get recovery suggestions based on error type and message"""
    suggestions = []
    
    if "connection" in error_msg.lower() or "ConnectionError" in error_type:
        suggestions.extend([
            "Verify SQL warehouse/endpoint is running",
            "Check DATABRICKS_HTTP_PATH is correct",
            "Ensure App Resources are configured in Databricks Apps settings",
            "Verify network connectivity"
        ])
    
    if "authentication" in error_msg.lower() or "auth" in error_msg.lower():
        suggestions.extend([
            "Verify DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET are set",
            "Check App Resources configuration",
            "Ensure service principal has proper permissions"
        ])
    
    if "notebook" in error_msg.lower() or "path" in error_msg.lower():
        suggestions.extend([
            "Verify notebook files exist in the expected location",
            "Check notebook paths in app configuration",
            "Ensure notebooks are accessible"
        ])
    
    if "job" in error_msg.lower() or "submit" in error_msg.lower():
        suggestions.extend([
            "Check job configuration and parameters",
            "Verify compute resources are available",
            "Review job logs for detailed error information"
        ])
    
    if "permission" in error_msg.lower() or "access" in error_msg.lower():
        suggestions.extend([
            "Verify service principal has required permissions",
            "Check Unity Catalog access permissions",
            "Ensure proper resource access in App Resources"
        ])
    
    return suggestions


def handle_error(error: Exception, context: str = "", show_traceback: bool = False) -> str:
    """
    Handle error and return formatted message
    
    Args:
        error: The exception
        context: Context where error occurred
        show_traceback: Whether to include traceback
    
    Returns:
        Formatted error message
    """
    message = format_error_message(error, context)
    
    if show_traceback:
        tb = traceback.format_exc()
        message += f"\n\n**Traceback:**\n```\n{tb}\n```"
    
    return message
