def get_human_readable_size(size_bytes):
    """Convert size in bytes to human readable format."""
    try:
        size_bytes = int(size_bytes)  # Ensure size_bytes is an integer
        if not isinstance(size_bytes, (int, float)) or size_bytes < 0:
            return "Unknown size"
        
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.2f} PB"
    except (ValueError, TypeError):
        return "Unknown size" 