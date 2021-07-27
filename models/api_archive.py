from app.database import db
from config import ConfigClass

class ArchivePreviewModel(db.Model):
    __tablename__ = "archive_preview"
    __table_args__ = {"schema": ConfigClass.RDS_SCHEMA_DEFAULT}
    id = db.Column(db.Integer, unique=True, primary_key=True)
    file_geid = db.Column(db.String())
    archive_preview = db.Column(db.String())

    def __init__(self, file_geid, archive_preview):
        self.file_geid = file_geid
        self.archive_preview = archive_preview

    def to_dict(self):
        result = {}
        for field in ["id", "file_geid", "archive_preview"]:
            result[field] = str(getattr(self, field))
        return result
