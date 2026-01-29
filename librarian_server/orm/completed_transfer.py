from .. import database as db
import datetime
from hera_librarian.models.transfer import CompletedTransferCore

class CompletedTransfer(db.Base):
    """
    The SQLAlchemy ORM model for a completed transfer.
    """
    __tablename__ = "completed_transfers"

    id: int = db.Column(db.Integer, db.ForeignKey("send_queue.id"), primary_key=True)

    send_queue = db.relationship("SendQueue", back_populates="completed_record")

    task_id: str = db.Column(db.String(256), nullable=False, unique=True)

    source_endpoint_id: str = db.Column(db.String(256), nullable=False)

    destination_endpoint_id = db.Column(db.String(256), nullable=False)

    start_time: datetime.datetime = db.Column(db.DateTime, nullable=False)

    end_time: datetime.datetime = db.Column(db.DateTime, nullable=False)

    duration_seconds: float = db.Column(db.Integer, nullable=False)

    bytes_transferred: int = db.Column(db.BigInteger, nullable=False)

    effective_bandwidth_bps: float = db.Column(db.Integer, nullable=False)

    send_queue = db.relationship("SendQueue", back_populates="completed_record")

    @classmethod
    def from_core(cls, core: CompletedTransferCore, queue_id: int) -> "CompletedTransfer":
        """
        Creates a new CompletedTransfer ORM object from a core model.
        """
        return cls(
            id=queue_id,
            task_id=core.task_id,
            source_endpoint_id=core.source_endpoint_id,
            destination_endpoint_id=core.destination_endpoint_id,
            start_time=core.start_time,
            end_time=core.end_time,
            duration_seconds=core.duration_seconds,
            bytes_transferred=core.bytes_transferred,
            effective_bandwidth_bps=core.effective_bandwidth_bps,
        )