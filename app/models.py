from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass
class DownloadTask:
    id: str
    url: str
    type: str  # "video" | "audio"
    format: str
    quality: str
    save_path: str
    filename_template: str

    # Subtitle options
    subtitles_download: bool = False  # download subtitle files (.srt)
    subtitles_embed: bool = False     # embed subtitles into video file (MP4/MKV)
    subtitles_burnin: bool = False    # burn-in subtitles into video (hardcode, requires re-encode)
    subtitle_only: bool = False       # skip media download and only process subtitles on existing local media
    local_video_subtitles: bool = False  # process existing local video files without any network subtitle fetch
    subtitle_existing_only: bool = False  # only use existing local subtitle files, do not download/translate
    subtitles_langs: str = "en"      # comma-separated language codes
    subtitles_translate: str = ""     # translation mode: "" (none), "zh" (Chinese only), "bilingual" (Chinese + English)
    subtitles_codex_strict: bool = False  # if true, translation must use Codex and may not fallback
    subtitles_transcribe_missing: bool = False  # generate English SRT locally when no source subtitles exist
    subtitles_bilingual_layout: str = ""  # "" (default bottom layout), "split_cn_top_en_bottom"
    subtitles_review_mode: bool = False  # pause after translation for manual review before burn-in
    subtitle_note: str = ""      # non-fatal note about subtitle processing
    subtitle_file_path: str = ""  # path to subtitle file (for review mode)
    subtitle_target_files: List[str] = field(default_factory=list)  # optional: only process these local media files
    subtitle_missing_files: List[str] = field(default_factory=list)  # batch mode: media files missing usable subtitles
    subtitle_failed_files: List[str] = field(default_factory=list)   # batch mode: media files failed in translate/embed/burn-in
    subtitle_processed_files: int = 0
    subtitle_skipped_files: int = 0
    subtitle_failed_count: int = 0

    status: str = "queued"  # queued/preparing/downloading/processing/completed/error/paused/canceled
    progress: float = 0.0
    speed: str = "0 KB/s"
    eta: str = "--"

    title: str = "解析中..."
    uploader: str = ""
    filename: str = ""
    total_size: str = "--"
    thumbnail: str = ""

    error_message: str = ""
    engine_log: List[str] = field(default_factory=list)
    output_files: List[str] = field(default_factory=list)

    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)

    def touch(self) -> None:
        self.updated_at = datetime.utcnow()

    def to_public_dict(self) -> Dict[str, Any]:
        is_video = self.type == "video"
        return {
            "task_id": self.id,
            "url": self.url,
            "type": self.type,
            "format": self.format,
            "quality": self.quality,
            "save_path": self.save_path,
            "status": self.status,
            "progress": round(float(self.progress or 0.0), 1),
            "speed": self.speed,
            "eta": self.eta,
            "title": self.title,
            "uploader": self.uploader,
            "filename": self.filename,
            "total_size": self.total_size,
            "thumbnail": self.thumbnail,
            "engine_log": list((self.engine_log or [])[-200:]),
            "output_files": list(self.output_files or []),
            "subtitles_download": self.subtitles_download if is_video else False,
            "subtitles_embed": self.subtitles_embed if is_video else False,
            "subtitles_burnin": self.subtitles_burnin if is_video else False,
            "subtitle_only": self.subtitle_only if is_video else False,
            "local_video_subtitles": self.local_video_subtitles if is_video else False,
            "subtitle_existing_only": self.subtitle_existing_only if is_video else False,
            "subtitles_langs": self.subtitles_langs,
            "subtitles_translate": self.subtitles_translate if is_video else "",
            "subtitles_codex_strict": bool(self.subtitles_codex_strict) if is_video else False,
            "subtitles_transcribe_missing": bool(self.subtitles_transcribe_missing) if is_video else False,
            "subtitles_bilingual_layout": self.subtitles_bilingual_layout if is_video else "",
            "subtitles_review_mode": self.subtitles_review_mode if is_video else False,
            "subtitle_note": self.subtitle_note if is_video else "",
            "subtitle_file_path": self.subtitle_file_path if is_video else "",
            "subtitle_target_files": self.subtitle_target_files if is_video else [],
            "subtitle_missing_files": self.subtitle_missing_files if is_video else [],
            "subtitle_failed_files": self.subtitle_failed_files if is_video else [],
            "subtitle_processed_files": int(self.subtitle_processed_files or 0) if is_video else 0,
            "subtitle_skipped_files": int(self.subtitle_skipped_files or 0) if is_video else 0,
            "subtitle_failed_count": int(self.subtitle_failed_count or 0) if is_video else 0,
            "error_message": self.error_message,
            "created_at": (self.created_at or datetime.utcnow()).isoformat(),
            "updated_at": (self.updated_at or datetime.utcnow()).isoformat(),
        }


@dataclass
class TaskControl:
    pause_requested: bool = False
    cancel_requested: bool = False
    delete_files_on_cancel: bool = False


class TaskPause(Exception):
    pass


class TaskCancel(Exception):
    pass
