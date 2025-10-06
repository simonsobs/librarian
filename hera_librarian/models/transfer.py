from pydantic import BaseModel
from datetime import datetime

class CompletedTransferCore(BaseModel):
    """
    A Pydantic model representing the data for a completed transfer.
    """

    task_id: str
    source_endpoint_id: str
    destination_endpoint_id: str
    start_time: datetime
    end_time: datetime
    duration_seconds: float 
    bytes_transferred: int 
    effective_bandwidth_bps: float