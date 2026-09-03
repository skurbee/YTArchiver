from __future__ import annotations

from backend.api_mixins.onboarding_mixin import OnboardingMixin


def test_onboarding_commits_unlimited_traffic_and_budget_autosync_together():
    state = {
        "youtube_traffic_mode": "balanced",
        "autorun_interval": -1,
    }

    class Api(OnboardingMixin):
        def __init__(self):
            self._reload_config = lambda: None

        def _onboarding_update_config(self, mutator):
            mutator(state)
            return None, dict(state)

    result = Api().onboarding_set_traffic("unlimited")

    assert result["ok"] is True
    assert result["budget_autosync_disabled"] is True
    assert state["youtube_traffic_mode"] == "unlimited"
    assert state["autorun_interval"] == 0
