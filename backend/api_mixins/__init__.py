"""api_mixins — handler classes extracted from main.py Api class."""

from .archive_mixin import ArchiveMixin
from .backup_mixin import BackupMixin
from .bookmark_mixin import BookmarkMixin
from .browse_mixin import BrowseMixin
from .channel_mixin import ChannelMixin
from .diagnostics_mixin import DiagnosticsMixin
from .index_mixin import IndexMixin
from .info_mixin import InfoMixin
from .livestreams_mixin import LivestreamsMixin
from .media_ops_mixin import MediaOpsMixin
from .metadata_mixin import MetadataMixin
from .onboarding_mixin import OnboardingMixin
from .queue_mixin import QueueMixin
from .recent_mixin import RecentMixin
from .redownload_mixin import RedownloadMixin
from .settings_mixin import SettingsMixin
from .startup_mixin import StartupMixin
from .subs_mixin import SubsMixin
from .sync_mixin import SyncMixin
from .thumbnail_mixin import ThumbnailMixin
from .transcribe_mixin import TranscribeMixin
from .video_mixin import VideoMixin
from .window_mixin import WindowMixin

__all__ = [
    "ArchiveMixin",
    "BackupMixin",
    "BookmarkMixin",
    "BrowseMixin",
    "ChannelMixin",
    "DiagnosticsMixin",
    "IndexMixin",
    "InfoMixin",
    "LivestreamsMixin",
    "MediaOpsMixin",
    "MetadataMixin",
    "OnboardingMixin",
    "QueueMixin",
    "RecentMixin",
    "RedownloadMixin",
    "SettingsMixin",
    "StartupMixin",
    "SubsMixin",
    "SyncMixin",
    "ThumbnailMixin",
    "TranscribeMixin",
    "VideoMixin",
    "WindowMixin",
]
